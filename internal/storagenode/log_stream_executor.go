package storagenode

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode -package storagenode -destination log_stream_executor_mock.go . Timestamper,Sealer,Unsealer,Syncer,LogStreamExecutor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

var (
	errLSEClosed = errors.New("logstream: closed log stream executor")
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

	// lock guard for knownNextGLSN and uncommittedLLSNBegin
	// knownNextGLSN and uncommittedLLSNBegin should be updated simultaneously.
	mu sync.RWMutex

	// globalHighwatermark acts as a version of commit result. In the scope of the same
	// globalHighwatermark, all the reports requested by MR have the same uncommittedLLSNBegin.
	// Because the reports with different values of uncommittedLLSNBegin with the same
	// globalHighwatermark can make some log entries in the log stream have no or more than one GLSN.
	// It is read by GetReport, and written by commit.
	globalHighwatermark types.GLSN // set by Commit
	// knownNextGLSN types.GLSN // set by Commit

	// uncommittedLLSNBegin is the start of the range that is reported as uncommitted log
	// entries. In the reports marked as the same KnownNextGLSN, uncommittedLLSNBegin of those
	// must be the same value. Because MR calculates commit results based on the KnownNextGLSN
	// and uncommittedLLSNBegin.
	// After all of the commits in a commit result are completed successfully, the value of
	// uncommittedLLSNBegin should be increased by the difference between committedGLSNEnd and
	// committedGLSNBegin in the commit result.
	// It is read by GetReport and written by commit.
	uncommittedLLSNBegin types.LLSN

	// committedLLSNEnd is the next position to be committed to storage. It is used only by
	// commit function.
	// Sometimes, it can be greater than uncommittedLLSNBegin. See the reason why below.
	// It is read and written only by commit.
	committedLLSNEnd types.LLSN // increased only by Commit

	// uncommittedLLSNEnd is the tail of the LogStreamExecutor. It indicates the next position
	// for the log entry to be written. It is increased after successful writing to the storage
	// during the append operation.
	// It is read by GetReport, written by Append
	uncommittedLLSNEnd types.AtomicLLSN

	// atomic access
	// committedGLSNBegin   types.GLSN // R: read & subscribe W: Trim
	// lastCommittedGLSN types.GLSN // R: read W: Commit
	// committedGLSNBegin and committedGLSNEnd are knowledge which are learned from Trim and
	// Commit operations.
	//
	// localLowWatermark and committedGLSNEnd are knowledge that is learned from Trim and
	// Commit operations. Exact values of them are maintained in the storage. By learning
	// these values in the LogStreamExecutor, it can be avoided to unnecessary requests to
	// the storage.
	localLowWatermark  types.AtomicGLSN
	localHighWatermark types.AtomicGLSN

	trackers appendTaskTracker

	status   varlogpb.LogStreamStatus
	muStatus sync.RWMutex
	muSeal   sync.Mutex

	syncTracker   map[types.StorageNodeID]*SyncTaskStatus
	muSyncTracker sync.Mutex

	tst Timestamper

	logger  *zap.Logger
	options *LogStreamExecutorOptions
}

func NewLogStreamExecutor(logger *zap.Logger, logStreamID types.LogStreamID, storage Storage, options *LogStreamExecutorOptions) (LogStreamExecutor, error) {
	if storage == nil {
		return nil, fmt.Errorf("logstream: no storage")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("logstreamexecutor").With(zap.Any("lsid", logStreamID))

	lse := &logStreamExecutor{
		logStreamID:          logStreamID,
		storage:              storage,
		replicator:           NewReplicator(logStreamID, logger),
		stopped:              make(chan struct{}),
		runner:               runner.New(fmt.Sprintf("logstreamexecutor-%v", logStreamID), logger),
		trackers:             newAppendTracker(),
		appendC:              make(chan *appendTask, options.AppendCSize),
		trimC:                make(chan *trimTask, options.TrimCSize),
		commitC:              make(chan commitTask, options.CommitCSize),
		globalHighwatermark:  types.InvalidGLSN,
		uncommittedLLSNBegin: types.MinLLSN,
		uncommittedLLSNEnd:   types.AtomicLLSN(types.MinLLSN),
		committedLLSNEnd:     types.MinLLSN,
		localLowWatermark:    types.AtomicGLSN(types.MinGLSN),
		localHighWatermark:   types.AtomicGLSN(types.InvalidGLSN),
		status:               varlogpb.LogStreamStatusRunning,
		syncTracker:          make(map[types.StorageNodeID]*SyncTaskStatus),
		tst:                  NewTimestamper(),
		logger:               logger,
		options:              options,
	}
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

func (lse *logStreamExecutor) Run(ctx context.Context) error {
	lse.muRunning.Lock()
	defer lse.muRunning.Unlock()

	if lse.running {
		return nil
	}
	lse.running = true

	mctx, cancel := lse.runner.WithManagedCancel(ctx)
	lse.cancel = cancel

	var err error
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
	return lse.localHighWatermark.Load()
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

	localHWM := lse.localHighWatermark.Load()
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

	if lse.status == varlogpb.LogStreamStatusSealed {
		lse.tst.Touch()
		lse.status = varlogpb.LogStreamStatusRunning
		return nil
	}
	return fmt.Errorf("logstream: unseal error (status=%v)", lse.status)
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
	lwm := lse.localLowWatermark.Load()
	if glsn < lwm {
		return errTrimmed(glsn, lwm)
	}
	return nil
}

func (lse *logStreamExecutor) commitUndecidable(glsn types.GLSN) error {
	hwm := lse.localHighWatermark.Load()
	if glsn > hwm {
		return errUndecidable(glsn, hwm)
	}
	return nil
	//return glsn > lse.localHighWatermark.Load()
}

func (lse *logStreamExecutor) read(t *readTask) {
	logEntry, err := lse.storage.Read(t.glsn)
	if err != nil {
		t.err = fmt.Errorf("logstreamexecutor read error (glsn=%v): %w", t.glsn, err)
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
				err := verrors.NewSimpleErrorf(verrors.ErrUnordered, "llsn next=%v read=%v", llsn+1, result.LogEntry.LLSN)
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
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if !lse.running {
		return types.InvalidGLSN, errLSEClosed
	}
	if lse.isSealed() {
		lse.logger.Debug("could not append", zap.Error(verrors.ErrSealed))
		return types.InvalidGLSN, verrors.ErrSealed
	}

	appendT := newAppendTask(data, replicas, types.InvalidLLSN, &lse.trackers)
	if err := lse.addAppendC(ctx, appendT); err != nil {
		lse.logger.Debug("could not add appendTask to appendC", zap.Error(err))
		return types.InvalidGLSN, err
	}

	tctx, cancel := context.WithTimeout(ctx, lse.options.CommitWaitTimeout)
	defer cancel()
	err := appendT.wait(tctx)
	if err != nil {
		lse.logger.Error("could not wait appendTask", zap.Error(err))
	}
	appendT.close()
	return appendT.getGLSN(), err
}

func (lse *logStreamExecutor) addAppendC(ctx context.Context, t *appendTask) error {
	if lse.isSealed() {
		lse.logger.Debug("could not append or replicate", zap.Error(verrors.ErrSealed))
		return verrors.ErrSealed
	}
	tctx, cancel := context.WithTimeout(ctx, lse.options.AppendCTimeout)
	defer cancel()
	select {
	case lse.appendC <- t:
		return nil
	case <-tctx.Done():
		lse.logger.Error("could not add appendTask to appendC", zap.Error(tctx.Err()))
		return tctx.Err()
	case <-lse.stopped:
		return errLSEClosed
	}
}

func (lse *logStreamExecutor) prepare(ctx context.Context, t *appendTask) {
	if lse.isSealed() {
		lse.logger.Debug("could not append or replicate", zap.Error(verrors.ErrSealed))
		t.notify(verrors.ErrSealed)
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
		llsn = lse.uncommittedLLSNEnd.Load()
	}

	if llsn != lse.uncommittedLLSNEnd.Load() {
		return fmt.Errorf("%w (llsn=%v uncommittedLLSNEnd=%v)", verrors.ErrCorruptLogStream, llsn, lse.uncommittedLLSNEnd.Load())
	}

	if err := lse.storage.Write(llsn, data); err != nil {
		return err
	}

	// NOTE (jun): Tracking the appendTask MUST be starting before incrementing
	// uncommittedLLSNEnd.
	// Let's assume that incrementing uncommittedLLSNEnd is happened before tracking the
	// appendTask. If GetReport request is arrived and context switch is occurred, it may be
	// replied that the LS has the new uncommitted log, however it is not tracked yet.
	// If, moreover, Commit corresponded to the report is arrived, its content contains the
	// uncommitted log whose appendTask is not tracked. In this case, client of that append
	// request may wait forever.
	defer lse.uncommittedLLSNEnd.Add(1)
	if primary {
		t.markWritten(llsn)

		lse.muStatus.RLock()
		defer lse.muStatus.RUnlock()

		switch lse.status {
		case varlogpb.LogStreamStatusRunning:
			lse.trackers.track(llsn, t)
			return nil
		case varlogpb.LogStreamStatusDeleted:
			lse.logger.Panic("invalid LogStreamStatus", zap.Any("status", lse.status))
			return verrors.ErrInternal
		default:
			lse.logger.Error("could not add appendTask to tracker", zap.Any("status", lse.status), zap.Any("llsn", llsn))
			return verrors.ErrSealed
		}
	}

	return nil
}

func (lse *logStreamExecutor) triggerReplication(ctx context.Context, t *appendTask) {
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
			err = ctx.Err()
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
		return errLSEClosed
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
		return tctx.Err()
	case <-lse.stopped:
		return errLSEClosed
	}
	return nil
}

func (lse *logStreamExecutor) trim(t *trimTask) {
	if err := lse.isTrimmed(t.glsn); err != nil {
		return
	}
	lse.localLowWatermark.Store(t.glsn + 1)
	if err := lse.storage.DeleteCommitted(t.glsn); err != nil {
		lse.logger.Error("could not trim", zap.Error(err))
	}
}

func (lse *logStreamExecutor) GetReport() UncommittedLogStreamStatus {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	// TODO: If this is sealed, ...
	lse.mu.RLock()
	offset := lse.uncommittedLLSNBegin
	hwm := lse.globalHighwatermark
	lse.mu.RUnlock()

	return UncommittedLogStreamStatus{
		LogStreamID:           lse.logStreamID,
		KnownHighWatermark:    hwm,
		UncommittedLLSNOffset: offset,
		UncommittedLLSNLength: uint64(lse.uncommittedLLSNEnd.Load() - offset),
	}
}

func (lse *logStreamExecutor) Commit(ctx context.Context, cr CommittedLogStreamStatus) {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if err := lse.verifyCommit(cr.PrevHighWatermark); err != nil {
		lse.logger.Error("could not commit", zap.Error(err))
		return
	}

	ct := commitTask{
		highWatermark:      cr.HighWatermark,
		prevHighWatermark:  cr.PrevHighWatermark,
		committedGLSNBegin: cr.CommittedGLSNOffset,
		committedGLSNEnd:   cr.CommittedGLSNOffset + types.GLSN(cr.CommittedGLSNLength),
	}

	tctx, cancel := context.WithTimeout(ctx, lse.options.CommitCTimeout)
	defer cancel()
	select {
	case lse.commitC <- ct:
	case <-tctx.Done():
		lse.logger.Error("could not send commitTask to commitC", zap.Error(tctx.Err()))
	case <-lse.stopped:
		lse.logger.Error("could not send commitTask to commitC", zap.Error(errLSEClosed))
	}
}

func (lse *logStreamExecutor) verifyCommit(prevHighWatermark types.GLSN) error {
	lse.mu.RLock()
	defer lse.mu.RUnlock()

	if lse.globalHighwatermark.Invalid() {
		return nil
	}
	if lse.globalHighwatermark == prevHighWatermark {
		return nil
	}
	return fmt.Errorf("logstream: highwatermark mismatch (globalHighwatermark=%v prevHighWatermark=%v)", lse.globalHighwatermark, prevHighWatermark)
}

func (lse *logStreamExecutor) commit(t commitTask) {
	if err := lse.verifyCommit(t.prevHighWatermark); err != nil {
		lse.logger.Error("could not commit", zap.Error(err))
		return
	}

	commitOk := true
	glsn := t.committedGLSNBegin

	for glsn < t.committedGLSNEnd {
		llsn := lse.committedLLSNEnd
		if err := lse.storage.Commit(llsn, glsn); err != nil {
			lse.logger.Error("could not commit", zap.Error(err))
			// NOTE: The LogStreamExecutor fails to commit Log entries that are
			// assigned GLSN by MR, for example, because of the storage failure.
			// In other replicated storage nodes, it can be okay.
			// Should we lock, that is, finalize this log stream or need other else
			// mechanisms?
			commitOk = false
			lse.sealItself()
			break
		}
		lse.committedLLSNEnd++
		// NB: Mutating localHighWatermark here is somewhat nasty.
		// Read operation to the log entry before getting a response
		// of appending it might succeed. To mitigate the subtle case,
		// we can mutate the localHighWatermark just before replying
		// to the append operation. But it is not a perfect solution.
		lse.localHighWatermark.Store(glsn)
		appendT, ok := lse.trackers.get(llsn)
		if ok {
			appendT.setGLSN(glsn)
			appendT.notify(nil)
		} else {
			lse.logger.Warn("committed, but cannot notify since no appendTask", zap.Any("llsn", llsn), zap.Any("glsn", glsn))
		}
		lse.logger.Debug("committed", zap.Any("llsn", llsn), zap.Any("glsn", glsn))
		glsn++
	}

	// NOTE: This is a very subtle case. MR assigns GLSNs to these log entries, but the storage
	// fails to commit it. Actually, these GLSNs are holes. See the above comments.
	offset := types.LLSN(0)
	for glsn < t.committedGLSNEnd {
		// NOTE: To avoid a race condition, read committedLLSNEnd inside this for-loop.
		// Empty commit whilst syncing as a destination doesn't traverse this for-loop.
		llsn := lse.committedLLSNEnd + offset
		appendT, ok := lse.trackers.get(llsn)
		if ok {
			appendT.setGLSN(glsn)
			appendT.notify(fmt.Errorf("%w: commit error (llsn=%v glsn=%v)", verrors.ErrCorruptLogStream, llsn, glsn))
		} else {
			lse.logger.Warn("failed to commit, but cannot notify since no appendTask", zap.Any("llsn", llsn), zap.Any("glsn", glsn))
		}
		offset++
		glsn++
		lse.logger.Debug("commit failed", zap.Any("llsn", llsn))
	}

	if commitOk {
		lse.mu.Lock()
		// NOTE: Even empty commit should update HWM since LSR aggregates all of the
		// replicas in the storage node and take the minimum value of each HWMs.
		lse.globalHighwatermark = t.highWatermark
		lse.uncommittedLLSNBegin += types.LLSN(t.committedGLSNEnd - t.committedGLSNBegin)
		if t.committedGLSNEnd-t.committedGLSNBegin > 0 {
			lse.logger.Debug("commit batch",
				zap.Any("glsn_begin", t.committedGLSNBegin),
				zap.Any("glsn_end", t.committedGLSNEnd),
				zap.Any("old_hwm", lse.globalHighwatermark-t.highWatermark),
				zap.Any("new_hwm", lse.globalHighwatermark),
				zap.Any("new_committed_llsn_end", lse.committedLLSNEnd),
			)
		} else {
			lse.logger.Debug("empty commit batch")
		}
		lse.mu.Unlock()
	}
}

func (lse *logStreamExecutor) Sync(ctx context.Context, replica Replica, lastGLSN types.GLSN) (*SyncTaskStatus, error) {
	lse.muRunning.RLock()
	defer lse.muRunning.RUnlock()
	if !lse.running {
		return nil, errLSEClosed
	}
	// TODO (jun): Delete SyncTaskStatus, but when?
	if status := lse.Status(); status != varlogpb.LogStreamStatusSealed {
		lse.logger.Error("bad status to sync", zap.Any("status", status))
		return nil, fmt.Errorf("bad status (%v) to sync", status)
	}

	lse.muSyncTracker.Lock()
	defer lse.muSyncTracker.Unlock()

	if sts, ok := lse.syncTracker[replica.StorageNodeID]; ok {
		sts.mu.RLock()
		defer sts.mu.RUnlock()
		return &SyncTaskStatus{
			Replica: sts.Replica,
			State:   sts.State,
			First:   sts.First,
			Last:    sts.Last,
			Current: sts.Current,
		}, nil
	}

	firstGLSN := lse.localLowWatermark.Load()
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

	if err := lse.runner.RunC(mctx, lse.syncer(mctx, sts)); err != nil {
		lse.logger.Error("could not run syncer", zap.Error(err))
		delete(lse.syncTracker, replica.StorageNodeID)
	}

	return sts, nil
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
				err = fmt.Errorf("unexpected LLSN: expected=%v actual=%v", first.LLSN+numLogs, result.LogEntry.LLSN)
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
			lse.logger.Info("syncer complete", zap.Any("first", first), zap.Any("last", last), zap.Any("current", current))
			sts.State = snpb.SyncStateComplete
		} else {
			lse.logger.Error("syncer failure", zap.Error(err), zap.Any("first", first), zap.Any("last", last), zap.Any("current", current))
			sts.State = snpb.SyncStateError
		}
		sts.Err = err
		sts.mu.Unlock()
	}
}

func (lse *logStreamExecutor) getLogPosition(glsn types.GLSN) (types.LLSN, error) {
	if glsn == types.InvalidGLSN {
		return types.InvalidLLSN, errors.New("invalid argument")
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
		return errLSEClosed
	}
	// TODO (jun): prevent from triggering Sync from multiple sources
	if status := lse.Status(); status != varlogpb.LogStreamStatusSealing {
		lse.logger.Error("bad status to syncreplicate", zap.Any("status", status))
		return fmt.Errorf("bad status (%v) to syncreplicate", status)
	}

	if err := lse.storage.Write(current.GetLLSN(), data); err != nil {
		lse.logger.Error("syncreplicate: could not write", zap.Error(err))
		return err
	}

	if err := lse.storage.Commit(current.GetLLSN(), current.GetGLSN()); err != nil {
		lse.logger.Error("syncreplicate: could not commit", zap.Error(err))
		return err
	}

	lse.logger.Debug("syncreplicate", zap.Any("first", first), zap.Any("last", last), zap.Any("current", current))

	// FIXME (jun): use more safe and better method to reset all state variables
	if current.Equal(last) {
		lse.logger.Debug("syncreplate complete")
		lse.mu.Lock()
		defer lse.mu.Unlock()
		lse.globalHighwatermark = current.GetGLSN()
		lse.uncommittedLLSNBegin = current.GetLLSN() + 1

		lse.committedLLSNEnd = current.GetLLSN() + 1
		lse.uncommittedLLSNEnd.Store(current.GetLLSN() + 1)
		lse.localLowWatermark.Store(first.GetGLSN())
		lse.localHighWatermark.Store(last.GetGLSN())
	}

	return nil
}
