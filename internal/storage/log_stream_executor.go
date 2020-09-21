package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/runner"
	vpb "github.com/kakao/varlog/proto/varlog"
	"go.uber.org/zap"
)

type SubscribeResult struct {
	logEntry varlog.LogEntry
	err      error
}

type Sealer interface {
	Seal(lastCommittedGLSN types.GLSN) (vpb.LogStreamStatus, types.GLSN)
}

type Unsealer interface {
	Unseal() error
}

type LogStreamExecutor interface {
	Run(ctx context.Context) error
	Close()

	LogStreamID() types.LogStreamID
	Status() vpb.LogStreamStatus

	Read(ctx context.Context, glsn types.GLSN) ([]byte, error)
	Subscribe(ctx context.Context, glsn types.GLSN) (<-chan SubscribeResult, error)
	Append(ctx context.Context, data []byte, backups ...Replica) (types.GLSN, error)
	Trim(ctx context.Context, glsn types.GLSN) error

	Replicate(ctx context.Context, llsn types.LLSN, data []byte) error

	GetReport() UncommittedLogStreamStatus
	Commit(ctx context.Context, commitResult CommittedLogStreamStatus)

	Sealer
	Unsealer
}

type readTask struct {
	glsn types.GLSN
	data []byte
	err  error
	done chan struct{}
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

	running   bool
	muRunning sync.Mutex
	cancel    context.CancelFunc
	runner    *runner.Runner

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
	// committedGLSNBegin and committedGLSNEnd are knowledges which are learned from Trim and
	// Commit operations.
	//
	// localLowWatermark and committedGLSNEnd are knowledge that is learned from Trim and
	// Commit operations. Exact values of them are maintained in the storage. By learning
	// these values in the LogStreamExecutor, it can be avoided to unnecessary requests to
	// the storage.
	localLowWatermark  types.AtomicGLSN
	localHighWatermark types.AtomicGLSN

	trackers appendTaskTracker

	status   vpb.LogStreamStatus
	muStatus sync.RWMutex
	muSeal   sync.Mutex

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
		runner:               runner.New(fmt.Sprintf("logstreamexecutor-%v", logStreamID), logger),
		trackers:             newAppendTracker(),
		appendC:              make(chan *appendTask, options.AppendCSize),
		trimC:                make(chan *trimTask, options.TrimCSize),
		commitC:              make(chan commitTask, options.CommitCSize),
		globalHighwatermark:  types.InvalidGLSN,
		uncommittedLLSNBegin: types.MinLLSN,
		uncommittedLLSNEnd:   types.AtomicLLSN(types.MinLLSN),
		committedLLSNEnd:     types.MinLLSN,
		localLowWatermark:    types.AtomicGLSN(types.InvalidGLSN),
		localHighWatermark:   types.AtomicGLSN(types.InvalidGLSN),
		status:               vpb.LogStreamStatusRunning,
		logger:               logger,
		options:              options,
	}
	return lse, nil
}

func (lse *logStreamExecutor) LogStreamID() types.LogStreamID {
	return lse.logStreamID
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
		goto err_out
	}
	if err = lse.runner.RunC(mctx, lse.dispatchTrimC); err != nil {
		lse.logger.Error("could not run dispatchTrimC", zap.Error(err))
		goto err_out
	}
	if err = lse.runner.RunC(mctx, lse.dispatchCommitC); err != nil {
		lse.logger.Error("could not run dispatchCommitC", zap.Error(err))
		goto err_out
	}
	if err = lse.replicator.Run(mctx); err != nil {
		lse.logger.Error("could not run replicator", zap.Error(err))
		goto err_out
	}
	return nil

err_out:
	cancel()
	lse.runner.Stop()
	return err
}

func (lse *logStreamExecutor) Close() {
	lse.muRunning.Lock()
	defer lse.muRunning.Unlock()

	if !lse.running {
		return
	}
	lse.running = false
	if lse.cancel != nil {
		lse.cancel()
	}
	lse.replicator.Close()
	lse.runner.Stop()
	lse.logger.Info("stop")
}

func (lse *logStreamExecutor) Status() vpb.LogStreamStatus {
	lse.muStatus.RLock()
	defer lse.muStatus.RUnlock()
	return lse.status
}

func (lse *logStreamExecutor) isSealed() bool {
	status := lse.Status()
	return status.Sealed()
}

func (lse *logStreamExecutor) Seal(lastCommittedGLSN types.GLSN) (vpb.LogStreamStatus, types.GLSN) {
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
	status := vpb.LogStreamStatusSealed
	if localHWM < lastCommittedGLSN {
		status = vpb.LogStreamStatusSealing
	}
	lse.seal(status, false)

	// delete uncommitted logs those positions are larger than lastCommittedGLSN
	if status == vpb.LogStreamStatusSealed {
		// Find lastCommittedLLSN by using lastCommittedGLSN
		logEntry, err := lse.storage.Read(lastCommittedGLSN)
		if err != nil {
			// MR may be ahead of LSE.
			lse.logger.Debug("could not read lastCommittedGLSN",
				zap.Any("lastCommittedGLSN", lastCommittedGLSN), zap.Error(err),
			)
		}
		lastCommittedLLSN := logEntry.LLSN

		// notify error to appenders whose positions are larger than lastCommittedGLSN
		lse.trackers.foreach(func(appendT *appendTask) {
			llsn := appendT.getLLSN()
			if llsn > lastCommittedLLSN {
				appendT.notify(varlog.ErrSealed)
			}
		})

		lse.deleteUncommitted(lastCommittedLLSN)
	}
	return status, localHWM
}

func (lse *logStreamExecutor) Unseal() error {
	lse.muStatus.Lock()
	defer lse.muStatus.Unlock()
	if lse.status == vpb.LogStreamStatusSealed {
		lse.status = vpb.LogStreamStatusRunning
		return nil
	}
	return fmt.Errorf("logstream: unseal error (status=%v)", lse.status)
}

func (lse *logStreamExecutor) sealItself() {
	lse.seal(vpb.LogStreamStatusSealing, true)
}

func (lse *logStreamExecutor) seal(status vpb.LogStreamStatus, itself bool) {
	lse.muStatus.Lock()
	defer lse.muStatus.Unlock()
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
func (lse *logStreamExecutor) Read(ctx context.Context, glsn types.GLSN) ([]byte, error) {
	if err := lse.isTrimmed(glsn); err != nil {
		return nil, err
	}
	// TODO: wait until decidable or return an error
	if err := lse.commitUndecidable(glsn); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	task := &readTask{
		glsn: glsn,
		done: done,
	}
	go lse.read(task)
	select {
	case <-done:
		return task.data, task.err
	case <-ctx.Done():
		return nil, ctx.Err()
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
		// t.err = newVarlogError(err, "read error: %v", t.glsn)
		t.err = fmt.Errorf("logstreamexecutor read error (glsn=%v): %w", t.glsn, err)
	}
	t.data = logEntry.Data
	// t.data, t.err = lse.storage.Read(t.glsn)
	close(t.done)
}

func (lse *logStreamExecutor) Subscribe(ctx context.Context, glsn types.GLSN) (<-chan SubscribeResult, error) {
	// TODO: verify glsn
	return lse.subscribe(ctx, glsn), nil
}

func (lse *logStreamExecutor) subscribe(ctx context.Context, glsn types.GLSN) <-chan SubscribeResult {
	c := make(chan SubscribeResult)
	go func() {
		defer close(c)
		scanner, err := lse.storage.Scan(glsn)
		if err != nil {
			c <- SubscribeResult{err: err}
			return
		}
		entry, err := scanner.Next()
		c <- SubscribeResult{logEntry: entry, err: err}
		if err != nil {
			return
		}
		llsn := entry.LLSN
		for {
			select {
			case <-ctx.Done():
				c <- SubscribeResult{err: ctx.Err()}
				return
			default:
				res := SubscribeResult{logEntry: varlog.InvalidLogEntry}
				entry, err := scanner.Next()
				if llsn+1 != entry.LLSN {
					res.err = varlog.NewSimpleErrorf(varlog.ErrUnordered, "llsn next=%v read=%v", llsn+1, entry.LLSN)
					// res.err = fmt.Errorf("%w (LLSN next=%v read=%v)", varlog.ErrUnordered, llsn+1, entry.LLSN)
					c <- res
					return
				}
				res.logEntry = entry
				res.err = err
				llsn = entry.LLSN
				c <- res
				if err != nil {
					return
				}
			}
		}
	}()
	return c
}

func (lse *logStreamExecutor) Replicate(ctx context.Context, llsn types.LLSN, data []byte) error {
	if lse.isSealed() {
		return varlog.ErrSealed
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
	if lse.isSealed() {
		lse.logger.Debug("could not append", zap.Error(varlog.ErrSealed))
		return types.InvalidGLSN, varlog.ErrSealed
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
		lse.logger.Debug("could not append or replicate", zap.Error(varlog.ErrSealed))
		return varlog.ErrSealed
	}
	tctx, cancel := context.WithTimeout(ctx, lse.options.AppendCTimeout)
	defer cancel()
	select {
	case lse.appendC <- t:
		return nil
	case <-tctx.Done():
		lse.logger.Error("could not add appendTask to appendC", zap.Error(tctx.Err()))
		return tctx.Err()
	}
}

func (lse *logStreamExecutor) prepare(ctx context.Context, t *appendTask) {
	if lse.isSealed() {
		lse.logger.Debug("could not append or replicate", zap.Error(varlog.ErrSealed))
		t.notify(varlog.ErrSealed)
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
		return fmt.Errorf("%w (llsn=%v uncommittedLLSNEnd=%v)", varlog.ErrCorruptLogStream, llsn, lse.uncommittedLLSNEnd.Load())
	}

	if err := lse.storage.Write(llsn, data); err != nil {
		return err
	}

	lse.uncommittedLLSNEnd.Add(1)

	if primary {
		t.markWritten(llsn)
		lse.muStatus.RLock()
		defer lse.muStatus.RUnlock()
		if lse.status == vpb.LogStreamStatusRunning {
			lse.trackers.track(llsn, t)
			return nil
		}
		return fmt.Errorf("could not add appendTask to tracker (status=%v llsn=%v)", lse.status, llsn)
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
	}
}

func (lse *logStreamExecutor) verifyCommit(prevHighWatermark types.GLSN) error {
	lse.mu.RLock()
	defer lse.mu.RUnlock()

	if lse.globalHighwatermark.Invalid() {
		return nil
		// return true
	}
	if lse.globalHighwatermark == prevHighWatermark {
		return nil
		// return true
	}
	return fmt.Errorf("logstream: highwatermark mismatch (globalHighwatermark=%v prevHighWatermark=%v)", lse.globalHighwatermark, prevHighWatermark)
	// return false

	// If the LSE is newbie knownNextGLSN is zero. In LSE-wise commit, it is unnecessary,
	/*
		ret := lse.globalHighwatermark.Invalid() || lse.globalHighwatermark == prevHighWatermark
		if !ret {
			lse.logger.Error("incorrect CR",
				zap.Uint64("known next GLSN", uint64(lse.globalHighwatermark)),
				zap.Uint64("prev next GLSN", uint64(prevHighWatermark)))
		}
		return ret
	*/
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
		}
		lse.logger.Debug("committed", zap.Any("llsn", llsn), zap.Any("glsn", glsn))
		glsn++
	}

	// NOTE: This is a very subtle case. MR assigns GLSNs to these log entries, but the storage
	// fails to commit it. Actually, these GLSNs are holes. See the above comments.
	llsn := lse.committedLLSNEnd
	for glsn < t.committedGLSNEnd {
		appendT, ok := lse.trackers.get(llsn)
		if ok {
			appendT.setGLSN(glsn)
			appendT.notify(fmt.Errorf("%w: commit error (llsn=%v glsn=%v)", varlog.ErrCorruptLogStream, llsn, glsn))
		}
		glsn++
		llsn++
		lse.logger.Debug("commit failed", zap.Any("llsn", llsn))
	}

	if commitOk {
		lse.mu.Lock()
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
