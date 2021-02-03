package storagenode

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode -package storagenode -destination log_stream_executor_mock.go . Timestamper,Sealer,Unsealer,Syncer,LogStreamExecutor

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/label"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/util/telemetry/trace"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

var (
	errLSEClosed = stderrors.New("logstream: closed")
)

type Timestamper interface {
	Created() time.Time
	LastUpdated() time.Time
	Touch()
}
type timestamper struct {
	created time.Time
	updated atomic.Value
}

func NewTimestamper() Timestamper {
	now := time.Now()
	ts := &timestamper{
		created: now,
	}
	ts.updated.Store(now)
	return ts
}

func (ts *timestamper) Created() time.Time {
	return ts.created
}

func (ts *timestamper) LastUpdated() time.Time {
	return ts.updated.Load().(time.Time)
}

func (ts *timestamper) Touch() {
	ts.updated.Store(time.Now())
}

type Sealer interface {
	Seal(lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN)
}

type Unsealer interface {
	Unseal() error
}

type SyncTaskStatus struct {
	Replica Replica
	State   snpb.SyncState
	First   snpb.SyncPosition
	Last    snpb.SyncPosition
	Current snpb.SyncPosition
	Err     error
	cancel  context.CancelFunc
	mu      sync.RWMutex
}

func (sts *SyncTaskStatus) copy() *SyncTaskStatus {
	sts.mu.RLock()
	defer sts.mu.RUnlock()
	return &SyncTaskStatus{
		Replica: sts.Replica,
		State:   sts.State,
		First:   sts.First,
		Last:    sts.Last,
		Current: sts.Current,
		Err:     sts.Err,
	}
}

type Syncer interface {
	Sync(ctx context.Context, replica Replica, lastGLSN types.GLSN) (*SyncTaskStatus, error)
	SyncReplicate(ctx context.Context, first, last, current snpb.SyncPosition, data []byte) error
}

type LogStreamExecutor interface {
	Sealer
	Unsealer
	Syncer
	Timestamper

	Run(ctx context.Context) error
	Close()

	Path() string
	LogStreamID() types.LogStreamID
	Status() varlogpb.LogStreamStatus
	HighWatermark() types.GLSN

	Read(ctx context.Context, glsn types.GLSN) (types.LogEntry, error)
	Subscribe(ctx context.Context, begin, end types.GLSN) (<-chan ScanResult, error)
	Append(ctx context.Context, data []byte, backups ...Replica) (types.GLSN, error)
	Trim(ctx context.Context, glsn types.GLSN) error

	Replicate(ctx context.Context, llsn types.LLSN, data []byte) error

	GetReport() UncommittedLogStreamStatus
	Commit(ctx context.Context, commitResult CommittedLogStreamStatus)
}

type readTask struct {
	glsn     types.GLSN
	logEntry types.LogEntry
	err      error
	done     chan struct{}
}

type trimTask struct {
	glsn types.GLSN
}

type commitTask struct {
	highWatermark      types.GLSN
	prevHighWatermark  types.GLSN
	committedGLSNBegin types.GLSN
	committedGLSNEnd   types.GLSN
}

// TODO:
// - handle read or subscribe operations competing with trim operations
type logStreamExecutor struct {
	logStreamID types.LogStreamID
	storage     Storage
	replicator  Replicator

	running     bool
	muRunning   sync.RWMutex
	cancel      context.CancelFunc
	runner      *runner.Runner
	stopped     chan struct{}
	onceStopped sync.Once

	appendC chan *appendTask
	commitC chan commitTask
	trimC   chan *trimTask

	lsc LogStreamContext

	trackers appendTaskTracker

	status   varlogpb.LogStreamStatus
	muStatus sync.RWMutex
	muSeal   sync.Mutex

	syncTracker   map[types.StorageNodeID]*SyncTaskStatus
	muSyncTracker sync.Mutex

	tst Timestamper

	tmStub  *telemetryStub
	logger  *zap.Logger
	options *LogStreamExecutorOptions
}

func NewLogStreamExecutor(logger *zap.Logger, logStreamID types.LogStreamID, storage Storage, tmStub *telemetryStub, options *LogStreamExecutorOptions) (LogStreamExecutor, error) {
	if storage == nil {
		return nil, errors.New("logstream: no storage")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("logstreamexecutor").With(zap.Any("lsid", logStreamID))

	lse := &logStreamExecutor{
		logStreamID: logStreamID,
		storage:     storage,
		replicator:  NewReplicator(logStreamID, logger),
		stopped:     make(chan struct{}),
		runner:      runner.New(fmt.Sprintf("logstreamexecutor-%v", logStreamID), logger),
		trackers:    newAppendTracker(),
		appendC:     make(chan *appendTask, options.AppendCSize),
		trimC:       make(chan *trimTask, options.TrimCSize),
		commitC:     make(chan commitTask, options.CommitCSize),
		status:      varlogpb.LogStreamStatusRunning,
		syncTracker: make(map[types.StorageNodeID]*SyncTaskStatus),
		tst:         NewTimestamper(),
		tmStub:      tmStub,
		logger:      logger,
		options:     options,
	}

	lse.lsc.committedLLSNEnd.mu.Lock()
	defer lse.lsc.committedLLSNEnd.mu.Unlock()

	lse.lsc.rcc.mu.Lock()
	defer lse.lsc.rcc.mu.Unlock()

	restored := true
	restoredMsg := "The last LogStreamContext is recovered"
	if restored = lse.storage.RestoreLogStreamContext(&lse.lsc); !restored {
		restoredMsg = "New LogStreamContext is created"
		initLogStreamContext(&lse.lsc)
	}

	lse.storage.RestoreStorage(lse.lsc.committedLLSNEnd.llsn-1, lse.lsc.localHighWatermark.Load())

	lse.logger.Info(restoredMsg,
		zap.Uint64("global_hwm", uint64(lse.lsc.rcc.globalHighwatermark)),
		zap.Uint64("uncommitted_llsn_begin", uint64(lse.lsc.rcc.uncommittedLLSNBegin)),
		zap.Uint64("uncommitted_llsn_end", uint64(lse.lsc.uncommittedLLSNEnd.Load())),
		zap.Uint64("committed_llsn_end", uint64(lse.lsc.committedLLSNEnd.llsn)),
		zap.Uint64("local_lwm", uint64(lse.lsc.localLowWatermark.Load())),
		zap.Uint64("local_hwm", uint64(lse.lsc.localHighWatermark.Load())),
	)
	return lse, nil
}

func (lse *logStreamExecutor) Path() string {
	return lse.storage.Path()
}

func (lse *logStreamExecutor) LogStreamID() types.LogStreamID {
	return lse.logStreamID
}

func (lse *logStreamExecutor) Created() time.Time {
	return lse.tst.Created()
}

func (lse *logStreamExecutor) LastUpdated() time.Time {
	return lse.tst.LastUpdated()
}

func (lse *logStreamExecutor) Touch() {
	lse.tst.Touch()
}

func (lse *logStreamExecutor) Run(ctx context.Context) (err error) {
	ctx, span := lse.tmStub.startSpan(ctx, "storagenode.(*LogStreamExecutor).Run")
	defer func() {
		if err == nil {
			span.SetStatus(codes.Ok, "")
		} else {
			span.RecordError(err)
		}
		span.End()
	}()

	lse.muRunning.Lock()
	defer lse.muRunning.Unlock()

	if lse.running {
		return nil
	}
	lse.running = true

	mctx, cancel := lse.runner.WithManagedCancel(context.Background())
	lse.cancel = cancel

	if err = lse.runner.RunC(mctx, lse.dispatchAppendC); err != nil {
		lse.logger.Error("could not run dispatchAppendC", zap.Error(err))
		goto errOut
	}
	if err = lse.runner.RunC(mctx, lse.dispatchTrimC); err != nil {
		lse.logger.Error("could not run dispatchTrimC", zap.Error(err))
		goto errOut
	}
	if err = lse.runner.RunC(mctx, lse.dispatchCommitC); err != nil {
		lse.logger.Error("could not run dispatchCommitC", zap.Error(err))
		goto errOut
	}
	if err = lse.replicator.Run(mctx); err != nil {
		lse.logger.Error("could not run replicator", zap.Error(err))
		goto errOut
	}
	return nil

errOut:
	cancel()
	lse.runner.Stop()
	return err
}

func (lse *logStreamExecutor) Close() {
	lse.muRunning.RLock()
	cancel := lse.cancel
	lse.muRunning.RUnlock()
	if cancel != nil {
		cancel()
	}
	lse.replicator.Close()
	lse.runner.Stop()
	lse.stop()
	lse.muRunning.Lock()
	defer lse.muRunning.Unlock()

	if !lse.running {
		return
	}
	lse.running = false

	lse.exhaustAppendTrackers()
	if err := lse.storage.Close(); err != nil {
		lse.logger.Warn("error while closing storage", zap.Error(err))
	}
	lse.logger.Info("stop")
}

func (lse *logStreamExecutor) stop() {
	lse.onceStopped.Do(func() {
		lse.logger.Info("stopped rpc handlers")
		close(lse.stopped)
	})
}

func (lse *logStreamExecutor) exhaustAppendTrackers() {
	lse.logger.Info("exhaust pending append tasks")
	lse.trackers.foreach(func(appendT *appendTask) {
		lse.logger.Debug("discard append task", zap.Any("llsn", appendT.getLLSN()))
		appendT.notify(errLSEClosed)
	})
}

func (lse *logStreamExecutor) Status() varlogpb.LogStreamStatus {
	lse.muStatus.RLock()
	defer lse.muStatus.RUnlock()
	return lse.status
}

func (lse *logStreamExecutor) HighWatermark() types.GLSN {
	return lse.lsc.localHighWatermark.Load()
}

func (lse *logStreamExecutor) isSealed() bool {
	status := lse.Status()
	return status.Sealed()
}

func (lse *logStreamExecutor) Seal(lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN) {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if !lse.running {
		// TODO (jun): add an error to return values
		lse.logger.Error("could not seal", zap.Error(errLSEClosed))
		return lse.Status(), types.InvalidGLSN
	}
	// process seal request one by one
	lse.muSeal.Lock()
	defer lse.muSeal.Unlock()

	localHWM := lse.lsc.localHighWatermark.Load()
	if localHWM > lastCommittedGLSN {
		lse.logger.Panic("localHighWatermark > lastCommittedGLSN",
			zap.Any("localHighWatermark", localHWM),
			zap.Any("lastCommittedGLSN", lastCommittedGLSN),
		)
	}
	status := varlogpb.LogStreamStatusSealed
	if localHWM < lastCommittedGLSN {
		status = varlogpb.LogStreamStatusSealing
	}
	lse.seal(status, false)

	// delete uncommitted logs those positions are larger than lastCommittedGLSN
	if status == varlogpb.LogStreamStatusSealed {
		lastCommittedLLSN := types.InvalidLLSN
		// Find lastCommittedLLSN by using lastCommittedGLSN
		logEntry, err := lse.storage.Read(lastCommittedGLSN)
		if err != nil {
			// MR may be ahead of LSE.
			lse.logger.Debug("could not read lastCommittedGLSN",
				zap.Any("lastCommittedGLSN", lastCommittedGLSN), zap.Error(err),
			)
		} else {
			lastCommittedLLSN = logEntry.LLSN
		}

		// notify error to appenders whose positions are larger than lastCommittedGLSN
		lse.trackers.foreach(func(appendT *appendTask) {
			llsn := appendT.getLLSN()
			if llsn > lastCommittedLLSN {
				appendT.notify(verrors.ErrSealed)
			}
		})

		lse.deleteUncommitted(lastCommittedLLSN)
		lse.lsc.rcc.mu.Lock()
		lse.lsc.rcc.uncommittedLLSNBegin = lastCommittedLLSN + 1
		lse.lsc.uncommittedLLSNEnd.Store(lastCommittedLLSN + 1)
		lse.lsc.rcc.mu.Unlock()
	}
	return status, localHWM
}

func (lse *logStreamExecutor) Unseal() error {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if !lse.running {
		return errLSEClosed
	}
	lse.muStatus.Lock()
	defer lse.muStatus.Unlock()

	switch lse.status {
	case varlogpb.LogStreamStatusRunning:
		return nil
	case varlogpb.LogStreamStatusSealed:
		lse.tst.Touch()
		lse.status = varlogpb.LogStreamStatusRunning
		return nil
	default:
		return fmt.Errorf("logstream: unseal error (status=%v)", lse.status)
	}
}

func (lse *logStreamExecutor) sealItself() {
	lse.seal(varlogpb.LogStreamStatusSealing, true)
}

func (lse *logStreamExecutor) seal(status varlogpb.LogStreamStatus, itself bool) {
	lse.muStatus.Lock()
	defer lse.muStatus.Unlock()

	lse.tst.Touch()
	if !itself || lse.status.Running() {
		lse.status = status
	}
}

func (lse *logStreamExecutor) deleteUncommitted(lastCommittedLLSN types.LLSN) {
	lse.logger.Debug("delete written logs > lastCommittedLLSN", zap.Any("lastCommittedLLSN", lastCommittedLLSN))
	if err := lse.storage.DeleteUncommitted(lastCommittedLLSN + 1); err != nil {
		lse.logger.Panic("could not delete uncommitted logs", zap.Any("begin", lastCommittedLLSN+1), zap.Error(err))
	}
}

func (lse *logStreamExecutor) dispatchAppendC(ctx context.Context) {
	for {
		select {
		case t := <-lse.appendC:
			lse.prepare(ctx, t)
		case <-ctx.Done():
			return
		}
	}
}

func (lse *logStreamExecutor) dispatchTrimC(ctx context.Context) {
	for {
		select {
		case t := <-lse.trimC:
			lse.trim(t)
		case <-ctx.Done():
			return
		}
	}
}

func (lse *logStreamExecutor) dispatchCommitC(ctx context.Context) {
	for {
		select {
		case t := <-lse.commitC:
			lse.commit(t)
		case <-ctx.Done():
			return
		}
	}
}

// Read reads log entry located at given GLSN. Multiple read operations can be processed
// simultaneously regardless of other operations.
//
// FIXME: This is dummy implementation.
// - spinining early-read to decide NOENT or OK
// - read aggregation to minimize I/O
// - cache or not (use memstore?)
func (lse *logStreamExecutor) Read(ctx context.Context, glsn types.GLSN) (types.LogEntry, error) {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if !lse.running {
		return types.InvalidLogEntry, errLSEClosed
	}
	if err := lse.isTrimmed(glsn); err != nil {
		return types.InvalidLogEntry, err
	}
	// TODO: wait until decidable or return an error
	if err := lse.commitUndecidable(glsn); err != nil {
		return types.InvalidLogEntry, err
	}
	done := make(chan struct{})
	task := &readTask{
		logEntry: types.InvalidLogEntry,
		glsn:     glsn,
		done:     done,
	}
	go lse.read(task)
	select {
	case <-done:
		return task.logEntry, task.err
	case <-ctx.Done():
		return types.InvalidLogEntry, ctx.Err()
	case <-lse.stopped:
		return types.InvalidLogEntry, errLSEClosed
	}
}

func (lse *logStreamExecutor) isTrimmed(glsn types.GLSN) error {
	lwm := lse.lsc.localLowWatermark.Load()
	if glsn < lwm {
		return errTrimmed(glsn, lwm)
	}
	return nil
}

func (lse *logStreamExecutor) commitUndecidable(glsn types.GLSN) error {
	hwm := lse.lsc.localHighWatermark.Load()
	if glsn > hwm {
		// TODO (jun): consider below situation: local_hwm < trim's until < global_hwm
		// ISSUE: VARLOG-303
		return errUndecidable(glsn, hwm)
	}
	return nil
}

func (lse *logStreamExecutor) read(t *readTask) {
	logEntry, err := lse.storage.Read(t.glsn)
	if err != nil {
		t.err = errors.Wrapf(err, "logstream: glsn=%d", t.glsn)
	}
	t.logEntry = logEntry
	close(t.done)
}

func (lse *logStreamExecutor) Subscribe(ctx context.Context, begin, end types.GLSN) (<-chan ScanResult, error) {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if !lse.running {
		return nil, errLSEClosed
	}
	if begin >= end {
		return nil, verrors.ErrInvalid
	}

	// TODO (jun): begin has special meanings:
	// - begin == types.MinGLSN: subscribe to the earliest
	// - begin == types.MaxGLSN: subscribe to the latest
	if err := lse.isTrimmed(begin); err != nil {
		return nil, err
	}
	if err := lse.commitUndecidable(begin); err != nil {
		return nil, err
	}

	return lse.scan(ctx, begin, end)
}

// FIXME (jun): Use stopped channel
func (lse *logStreamExecutor) scan(ctx context.Context, begin, end types.GLSN) (<-chan ScanResult, error) {
	mctx, cancel := lse.runner.WithManagedCancel(context.Background())
	// TODO (jun): manages cancel functions
	resultC := make(chan ScanResult)
	if err := lse.runner.RunC(mctx, func(ctx context.Context) {
		defer cancel()
		defer close(resultC)

		scanner, err := lse.storage.Scan(begin, end)
		if err != nil {
			resultC <- NewInvalidScanResult(err)
			return
		}
		defer scanner.Close()

		result := scanner.Next()
		lse.logger.Debug("scan", zap.Any("result", result), zap.String("data", string(result.LogEntry.Data)))

		select {
		case resultC <- result:
		case <-ctx.Done():
			lse.logger.Error("scanner stopped", zap.Error(err))
			return
		}

		if !result.Valid() {
			lse.logger.Debug("scanner stopped", zap.Error(result.Err))
			return
		}

		llsn := result.LogEntry.LLSN
		for {
			if ctx.Err() != nil {
				resultC <- NewInvalidScanResult(ctx.Err())
				return
			}

			result := scanner.Next()
			lse.logger.Debug("scan", zap.Any("result", result), zap.String("data", string(result.LogEntry.Data)))

			if result.Valid() && llsn+1 != result.LogEntry.LLSN {
				// FIXME (jun): This situation is happened by two causes:
				// - Storage is broken: critical issue
				// - Trim is occurred: buggy or undecided behavior
				// It has no guarantee that read and subscribe prevent from trimming
				// overlapped log ranges yet. It, however, results in unexpected
				// situation like this.
				err := errors.Wrapf(verrors.ErrUnordered, "logstream: expected=%d, actual=%d", llsn+1, result.LogEntry.LLSN)
				result = NewInvalidScanResult(err)
			}
			select {
			case resultC <- result:
			case <-ctx.Done():
				lse.logger.Error("scanner stopped", zap.Error(err))
				return
			}

			if !result.Valid() {
				lse.logger.Debug("scanner stopped", zap.Error(result.Err))
				return
			}
			llsn = result.LogEntry.LLSN
		}
	}); err != nil {
		close(resultC)
		return nil, err
	}
	return resultC, nil
}

func (lse *logStreamExecutor) Replicate(ctx context.Context, llsn types.LLSN, data []byte) error {
	ctx, span := lse.tmStub.startSpan(ctx, "Replicate", oteltrace.WithAttributes(trace.LogStreamIDLabel(lse.logStreamID)))
	defer func() {
		span.End()
	}()

	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if !lse.running {
		return errLSEClosed
	}
	if lse.isSealed() {
		return verrors.ErrSealed
	}

	appendTask := newAppendTask(data, nil, llsn, &lse.trackers)
	if err := lse.addAppendC(ctx, appendTask); err != nil {
		return err
	}

	return appendTask.wait(ctx)
}

// Append appends a log entry at the end of the log stream. Append comprises of three parts -
// writing log entry into the underlying storage, replicating the log entry to backups, and
// waiting for commit completion event. After receiving commit completion event, Append returns
// given GLSN.
// If the log stream is locked, the append is failed.
// All Appends are processed sequentially by using the appendC.
func (lse *logStreamExecutor) Append(ctx context.Context, data []byte, replicas ...Replica) (types.GLSN, error) {
	commitWaitTime := int64(-1)

	ctx, span := lse.tmStub.startSpan(ctx, "Append", oteltrace.WithAttributes(trace.LogStreamIDLabel(lse.logStreamID)))
	defer func() {
		span.SetAttributes(label.Int64("commit_wait_time_ms", commitWaitTime))
		span.End()
	}()

	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if !lse.running {
		return types.InvalidGLSN, errors.WithStack(errLSEClosed)
	}
	if lse.isSealed() {
		return types.InvalidGLSN, errors.WithStack(verrors.ErrSealed)
	}

	appendT := newAppendTask(data, replicas, types.InvalidLLSN, &lse.trackers)
	appendT.span = span
	if err := lse.addAppendC(ctx, appendT); err != nil {
		return types.InvalidGLSN, err
	}

	tctx, cancel := context.WithTimeout(ctx, lse.options.CommitWaitTimeout)
	defer cancel()
	err := appendT.wait(tctx)
	appendT.close()
	commitWaitTime = appendT.commitWaitTime.Load().Milliseconds()
	return appendT.getGLSN(), err
}

func (lse *logStreamExecutor) addAppendC(ctx context.Context, t *appendTask) error {
	if lse.isSealed() {
		return errors.WithStack(verrors.ErrSealed)
	}
	tctx, cancel := context.WithTimeout(ctx, lse.options.AppendCTimeout)
	defer cancel()
	select {
	case lse.appendC <- t:
		return nil
	case <-tctx.Done():
		return errors.WithStack(tctx.Err())
	case <-lse.stopped:
		return errors.WithStack(errLSEClosed)
	}
}

func (lse *logStreamExecutor) prepare(ctx context.Context, t *appendTask) {
	ctx = oteltrace.ContextWithSpan(ctx, t.span)
	ctx, span := lse.tmStub.startSpan(ctx, "Prepare")
	defer func() {
		span.End()
	}()

	if lse.isSealed() {
		t.notify(errors.WithStack(verrors.ErrSealed))
		return
	}

	err := lse.write(t)
	if err != nil {
		lse.sealItself()
		t.notify(err)
		return
	}

	if t.isPrimary() {
		lse.triggerReplication(ctx, t)
	} else {
		t.notify(err)
	}
}

// append issues new LLSN for a LogEntry and write it to the storage. It, then, adds given
// appendTask to the taskmap to receive commit result. It also triggers replication after a
// successful write.
// In case of append, that is primary, it tries to add appendTask to tracker. If adding the
// appendTask to tracker is failed, write method fails.
func (lse *logStreamExecutor) write(t *appendTask) error {
	llsn, data, _ := t.getParams()
	primary := t.isPrimary()
	if primary {
		llsn = lse.lsc.uncommittedLLSNEnd.Load()
	}

	if llsn != lse.lsc.uncommittedLLSNEnd.Load() {
		return errors.Errorf("logstream: corrupt (llsn = %d, uncommittedLLSNEnd = %d)",
			llsn,
			lse.lsc.uncommittedLLSNEnd.Load(),
		)
		// return fmt.Errorf("%w (llsn=%v uncommittedLLSNEnd=%v)", verrors.ErrCorruptLogStream, llsn, lse.lsc.uncommittedLLSNEnd.Load())
	}

	if err := lse.storage.Write(llsn, data); err != nil {
		return err
	}

	t.writeCompletedTime.Store(time.Now())

	// NOTE (jun): Tracking the appendTask MUST be starting before incrementing
	// uncommittedLLSNEnd.
	// Let's assume that incrementing uncommittedLLSNEnd is happened before tracking the
	// appendTask. If GetReport request is arrived and context switch is occurred, it may be
	// replied that the LS has the new uncommitted log, however it is not tracked yet.
	// If, moreover, Commit corresponded to the report is arrived, its content contains the
	// uncommitted log whose appendTask is not tracked. In this case, client of that append
	// request may wait forever.
	defer lse.lsc.uncommittedLLSNEnd.Add(1)
	if primary {
		t.markWritten(llsn)

		lse.muStatus.RLock()
		defer lse.muStatus.RUnlock()

		switch lse.status {
		case varlogpb.LogStreamStatusRunning:
			lse.trackers.track(llsn, t)
			return nil
		case varlogpb.LogStreamStatusSealed, varlogpb.LogStreamStatusSealing:
			return errors.WithStack(verrors.ErrSealed)
		default:
			err := errors.Errorf("logstream: invalid status (%s)", lse.status)
			lse.logger.DPanic("invalid status", zap.Error(err))
			return err
		}
	}
	return nil
}

func (lse *logStreamExecutor) triggerReplication(ctx context.Context, t *appendTask) {
	ctx, span := lse.tmStub.startSpan(ctx, "TriggerReplication")
	defer func() {
		span.End()
	}()

	llsn, data, replicas := t.getParams()
	if len(replicas) == 0 {
		return
	}
	errC := lse.replicator.Replicate(ctx, llsn, data, replicas)
	go func() {
		// NOTE(jun): does it need timeout?
		var err error
		select {
		case err = <-errC:
		case <-ctx.Done():
			err = errors.Wrapf(ctx.Err(), "logstream")
		}
		if err != nil {
			lse.sealItself()
			t.notify(err)
		}
	}()
}

func (lse *logStreamExecutor) Trim(ctx context.Context, glsn types.GLSN) error {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if !lse.running {
		return errors.WithStack(errLSEClosed)
	}
	if err := lse.isTrimmed(glsn); err != nil {
		// already trimmed, no problem
		return nil
	}
	if err := lse.commitUndecidable(glsn); err != nil {
		// In case of Trim, errUndecidable means that the given glsn is larger than
		// localHighWatermark.
		return err
	}
	tctx, cancel := context.WithTimeout(ctx, lse.options.TrimCTimeout)
	defer cancel()
	select {
	case lse.trimC <- &trimTask{glsn: glsn}:
	case <-tctx.Done():
		return errors.Wrap(tctx.Err(), "logstream")
	case <-lse.stopped:
		return errors.WithStack(errLSEClosed)
	}
	return nil
}

func (lse *logStreamExecutor) trim(t *trimTask) {
	if err := lse.isTrimmed(t.glsn); err != nil {
		return
	}
	lse.lsc.localLowWatermark.Store(t.glsn + 1)
	if err := lse.storage.DeleteCommitted(t.glsn + 1); err != nil {
		lse.logger.Error("could not trim", zap.Error(err))
	}
}

func (lse *logStreamExecutor) GetReport() UncommittedLogStreamStatus {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()

	hwm, offset := lse.lsc.rcc.get()
	uncommittedLLSNEnd := lse.lsc.uncommittedLLSNEnd.Load()
	status := UncommittedLogStreamStatus{
		LogStreamID:           lse.logStreamID,
		KnownHighWatermark:    hwm,
		UncommittedLLSNOffset: offset,
		UncommittedLLSNLength: uint64(uncommittedLLSNEnd - offset),
	}
	lse.logger.Debug("get_report",
		zap.Any("hwm", status.KnownHighWatermark),
		zap.Any("llsn_offset", status.UncommittedLLSNOffset),
		zap.Any("llsn_length", status.UncommittedLLSNLength),
		zap.Any("uncommittedLLSNEnd", uncommittedLLSNEnd))

	return status
}

func (lse *logStreamExecutor) Commit(ctx context.Context, cr CommittedLogStreamStatus) {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()

	ct := commitTask{
		highWatermark:      cr.HighWatermark,
		prevHighWatermark:  cr.PrevHighWatermark,
		committedGLSNBegin: cr.CommittedGLSNOffset,
		committedGLSNEnd:   cr.CommittedGLSNOffset + types.GLSN(cr.CommittedGLSNLength),
	}

	if err := lse.verifyCommit(&ct); err != nil {
		lse.logger.Debug("could not commit", zap.Error(err))
		return
	}

	var err error
	tctx, cancel := context.WithTimeout(ctx, lse.options.CommitCTimeout)
	defer cancel()
	select {
	case lse.commitC <- ct:
		return
	case <-tctx.Done():
		err = errors.Wrapf(tctx.Err(), "logstream")
	case <-lse.stopped:
		err = errors.WithStack(errLSEClosed)
	}
	if err != nil {
		lse.logger.Error("could not commit", zap.Error(err))
	}
}

func (lse *logStreamExecutor) verifyCommit(ct *commitTask) error {
	lse.lsc.rcc.mu.RLock()
	defer lse.lsc.rcc.mu.RUnlock()

	uncommittedLLSNEnd := lse.lsc.uncommittedLLSNEnd.Load()
	numCommitted := uint64(ct.committedGLSNEnd - ct.committedGLSNBegin)
	numUncommitted := uint64(uncommittedLLSNEnd - lse.lsc.rcc.uncommittedLLSNBegin)
	if numUncommitted < numCommitted {
		// NOTE: MR just sends past commit messages to recovered SN that has no written logs.
		return errors.Errorf("logstream: numUncommitted (%d - %d = %d) < numCommitted (%d - %d = %d) - recovered sn?",
			uncommittedLLSNEnd, lse.lsc.rcc.uncommittedLLSNBegin, numUncommitted,
			ct.committedGLSNEnd, ct.committedGLSNBegin, numCommitted)
	}

	knownGlobalHWM := lse.lsc.rcc.globalHighwatermark
	if knownGlobalHWM != ct.prevHighWatermark {
		return errors.Errorf("logstream: highwatermark mismatch (LSE.globalHWM=%v Commit.prevHWM=%v)", knownGlobalHWM, ct.prevHighWatermark)
	}

	return nil
}

func (lse *logStreamExecutor) commit(t commitTask) {
	if err := lse.verifyCommit(&t); err != nil {
		lse.logger.Debug("could not commit", zap.Error(err))
		return
	}

	var (
		commitErr error
		nrCommits uint64

		first    = true
		commitOk = true
		glsn     = t.committedGLSNBegin
	)

	lse.lsc.committedLLSNEnd.mu.Lock()
	defer lse.lsc.committedLLSNEnd.mu.Unlock()

	for glsn < t.committedGLSNEnd {
		if first {
			cc := CommitContext{
				HighWatermark:      t.highWatermark,
				PrevHighWatermark:  t.prevHighWatermark,
				CommittedGLSNBegin: t.committedGLSNBegin,
				CommittedGLSNEnd:   t.committedGLSNEnd,
			}
			if commitErr = lse.storage.StoreCommitContext(cc); commitErr != nil {
				commitOk = false
				lse.sealItself()
				break
			}
			first = false
		}

		llsn := lse.lsc.committedLLSNEnd.llsn
		if commitErr = lse.storage.Commit(llsn, glsn); commitErr != nil {
			// NOTE: The LogStreamExecutor fails to commit Log entries that are
			// assigned GLSN by MR, for example, because of the storage failure.
			// In other replicated storage nodes, it can be okay.
			// Should we lock, that is, finalize this log stream or need other else
			// mechanisms?
			commitOk = false
			lse.sealItself()
			break
		}
		nrCommits++
		lse.lsc.committedLLSNEnd.llsn++
		// NB: Mutating localHighWatermark here is somewhat nasty.
		// Read operation to the log entry before getting a response
		// of appending it might succeed. To mitigate the subtle case,
		// we can mutate the localHighWatermark just before replying
		// to the append operation. But it is not a perfect solution.
		lse.lsc.localHighWatermark.Store(glsn)
		appendT, ok := lse.trackers.get(llsn)
		if ok {
			writeCompletedTime := appendT.writeCompletedTime.Load()
			if !writeCompletedTime.IsZero() {
				appendT.commitWaitTime.Store(time.Since(writeCompletedTime))
			}
			appendT.setGLSN(glsn)
			appendT.notify(nil)
		}
		lse.logger.Debug("commit",
			zap.Uint64("llsn", uint64(llsn)),
			zap.Uint64("glsn", uint64(glsn)),
			zap.Bool("notify", ok),
		)
		glsn++
	}

	// NOTE: This is a very subtle case. MR assigns GLSNs to these log entries, but the storage
	// fails to commit it. Actually, these GLSNs are holes. See the above comments.
	offset := types.LLSN(0)
	for glsn < t.committedGLSNEnd {
		// NOTE: To avoid a race condition, read committedLLSNEnd inside this for-loop.
		// Empty commit whilst syncing as a destination doesn't traverse this for-loop.
		llsn := lse.lsc.committedLLSNEnd.llsn + offset
		appendT, ok := lse.trackers.get(llsn)
		if ok {
			appendT.setGLSN(glsn)
			appendT.notify(errors.Errorf("logstream: could not commit (llsn = %d, glsn = %d)", llsn, glsn))
		}
		lse.logger.Debug("could not commit",
			zap.Uint64("llsn", uint64(llsn)),
			zap.Uint64("glsn", uint64(glsn)),
			zap.Bool("notify", ok),
		)
		offset++
		glsn++
	}

	if commitOk {
		// NOTE: Even empty commit should update HWM since LSR aggregates all of the
		// replicas in the storage node and take the minimum value of each HWMs.
		lse.lsc.rcc.mu.Lock()
		defer lse.lsc.rcc.mu.Unlock()

		total := t.committedGLSNEnd - t.committedGLSNBegin
		lse.lsc.rcc.globalHighwatermark = t.highWatermark
		lse.lsc.rcc.uncommittedLLSNBegin += types.LLSN(total)

		if lse.logger.Core().Enabled(zapcore.InfoLevel) {
			var sb strings.Builder

			fmt.Fprintf(&sb, "[%d, %d)", t.committedGLSNBegin, t.committedGLSNEnd)
			glsnRange := sb.String()
			sb.Reset()

			llsnEnd := lse.lsc.committedLLSNEnd.llsn
			fmt.Fprintf(&sb, "[%d, %d)", llsnEnd-types.LLSN(total), llsnEnd)
			llsnRange := sb.String()
			sb.Reset()

			fmt.Fprintf(&sb, "%d -> %d", t.prevHighWatermark, t.highWatermark)
			hwmUpdate := sb.String()

			lse.logger.Info("commit completed",
				zap.Uint64("nr_commits", uint64(total)),
				zap.String("glsn", glsnRange),
				zap.String("llsn", llsnRange),
				zap.String("global_hwm", hwmUpdate),
				zap.Uint64("local_hwm", uint64(lse.lsc.localHighWatermark.Load())),
			)
		}
		return
	}

	fields := make([]zap.Field, 0, 2)
	fields = append(fields, zap.Error(commitErr))
	if nrCommits > 0 {
		var sb strings.Builder
		fmt.Fprintf(&sb, "%d/%d", nrCommits, t.committedGLSNEnd-t.committedGLSNBegin)
		fields = append(fields, zap.String("partial_commits", sb.String()))
	}
	lse.logger.Error("commit failed", fields...)
}

func (lse *logStreamExecutor) Sync(ctx context.Context, replica Replica, lastGLSN types.GLSN) (*SyncTaskStatus, error) {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if !lse.running {
		return nil, errors.WithStack(errLSEClosed)
	}
	// TODO (jun): Delete SyncTaskStatus, but when?
	if status := lse.Status(); status != varlogpb.LogStreamStatusSealed {
		return nil, errors.Errorf("logstream: invalid status to sync (%s)", status)
	}

	lse.muSyncTracker.Lock()
	defer lse.muSyncTracker.Unlock()

	if sts, ok := lse.syncTracker[replica.StorageNodeID]; ok {
		sts.mu.RLock()
		defer sts.mu.RUnlock()
		// FIXME (jun): Deleting sync history this point is not good.
		if sts.Err != nil {
			delete(lse.syncTracker, replica.StorageNodeID)
		}
		return &SyncTaskStatus{
			Replica: sts.Replica,
			State:   sts.State,
			First:   sts.First,
			Last:    sts.Last,
			Current: sts.Current,
		}, nil
	}

	firstGLSN := lse.lsc.localLowWatermark.Load()
	firstLLSN, err := lse.getLogPosition(firstGLSN)
	if err != nil {
		return nil, err
	}
	lastLLSN, err := lse.getLogPosition(lastGLSN)
	if err != nil {
		return nil, err
	}

	first := snpb.SyncPosition{LLSN: firstLLSN, GLSN: firstGLSN}
	last := snpb.SyncPosition{LLSN: lastLLSN, GLSN: lastGLSN}
	current := snpb.SyncPosition{LLSN: types.InvalidLLSN, GLSN: types.InvalidGLSN}

	mctx, cancel := lse.runner.WithManagedCancel(context.Background())
	sts := &SyncTaskStatus{
		Replica: replica,
		State:   snpb.SyncStateInProgress,
		First:   first,
		Last:    last,
		Current: current,
		cancel:  cancel,
	}
	lse.syncTracker[replica.StorageNodeID] = sts

	err = lse.runner.RunC(mctx, lse.syncer(mctx, sts))
	if err != nil {
		err = errors.Wrapf(err, "logstream")
		delete(lse.syncTracker, replica.StorageNodeID)
	}

	return sts.copy(), err
}

func (lse *logStreamExecutor) syncer(ctx context.Context, sts *SyncTaskStatus) func(context.Context) {
	first, last, current := sts.First, sts.Last, sts.Current
	replica := sts.Replica
	return func(ctx context.Context) {
		defer sts.cancel()

		var err error
		numLogs := types.LLSN(0)
		resultC, err := lse.scan(ctx, first.GLSN, last.GLSN+1)
		if err != nil {
			// handle it
			goto errOut
		}

		for result := range resultC {
			if !result.Valid() {
				err = result.Err
				if err == ErrEndOfRange {
					err = nil
				}
				break
			}

			if result.LogEntry.LLSN != first.LLSN+numLogs {
				err = errors.Errorf("logstream: unexpected LLSN (expected = %v, actual = %v)", first.LLSN+numLogs, result.LogEntry.LLSN)
				break
			}
			numLogs++

			current = snpb.SyncPosition{
				LLSN: result.LogEntry.LLSN,
				GLSN: result.LogEntry.GLSN,
			}
			if err = lse.replicator.SyncReplicate(ctx, replica, first, last, current, result.LogEntry.Data); err != nil {
				break
			}

			// update status
			sts.mu.Lock()
			sts.Current = current
			sts.mu.Unlock()
		}

	errOut:
		sts.mu.Lock()
		if err == nil {
			lse.logger.Debug("syncer completed", zap.Any("first", first), zap.Any("last", last), zap.Any("current", current))
			sts.State = snpb.SyncStateComplete
		} else {
			lse.logger.Error("syncer failed", zap.Error(err), zap.Any("first", first), zap.Any("last", last), zap.Any("current", current))
			sts.State = snpb.SyncStateError
		}
		sts.Err = err
		sts.mu.Unlock()
	}
}

func (lse *logStreamExecutor) getLogPosition(glsn types.GLSN) (types.LLSN, error) {
	if glsn == types.InvalidGLSN {
		return types.InvalidLLSN, errors.New("logstream: invalid GLSN")
	}
	logEntry, err := lse.storage.Read(glsn)
	if err != nil {
		return types.InvalidLLSN, err
	}
	return logEntry.LLSN, nil

}

func (lse *logStreamExecutor) SyncReplicate(ctx context.Context, first, last, current snpb.SyncPosition, data []byte) error {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if !lse.running {
		return errors.WithStack(errLSEClosed)
	}
	// TODO (jun): prevent from triggering Sync from multiple sources
	if status := lse.Status(); status != varlogpb.LogStreamStatusSealing {
		return errors.Errorf("invalid status to syncreplicate (%s)", status)
	}

	// TODO: guard lse.lsc.committedLLSNEnd
	// TODO: prepare SyncReplicate: negotiate copy range
	lse.lsc.committedLLSNEnd.mu.RLock()
	committedLLSNEnd := lse.lsc.committedLLSNEnd.llsn
	lse.lsc.committedLLSNEnd.mu.RUnlock()
	if current.GetLLSN() < committedLLSNEnd {
		// Do not copy already committed logs
		return nil
	}

	if err := lse.storage.Write(current.GetLLSN(), data); err != nil {
		// lse.logger.Error("syncreplicate: could not write", zap.Error(err))
		return err
	}

	if err := lse.storage.Commit(current.GetLLSN(), current.GetGLSN()); err != nil {
		// lse.logger.Error("syncreplicate: could not commit", zap.Error(err))
		return err
	}

	lse.logger.Debug("syncreplicate", zap.Any("first", first), zap.Any("last", last), zap.Any("current", current))

	// FIXME (jun): use more safe and better method to reset all state variables
	if current.Equal(last) {
		lse.logger.Debug("syncreplate complete")

		lse.lsc.committedLLSNEnd.mu.Lock()
		defer lse.lsc.committedLLSNEnd.mu.Unlock()

		lse.lsc.rcc.mu.Lock()
		defer lse.lsc.rcc.mu.Unlock()

		lse.lsc.rcc.globalHighwatermark = current.GetGLSN()
		lse.lsc.rcc.uncommittedLLSNBegin = current.GetLLSN() + 1

		lse.lsc.committedLLSNEnd.llsn = current.GetLLSN() + 1
		lse.lsc.uncommittedLLSNEnd.Store(current.GetLLSN() + 1)
		lse.lsc.localLowWatermark.Store(first.GetGLSN())
		lse.lsc.localHighWatermark.Store(last.GetGLSN())
	}

	return nil
}
