package logstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

type committer struct {
	committerConfig

	commitWaitQ        *commitWaitQueue
	inflightCommitWait atomic.Int64

	commitQueue    chan *commitTask
	inflightCommit atomic.Int64

	runner *runner.Runner
}

// newCommitter creates a new committer.
func newCommitter(cfg committerConfig) (*committer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cm := &committer{
		committerConfig: cfg,
		commitWaitQ:     newCommitWaitQueue(),
		commitQueue:     make(chan *commitTask, cfg.commitQueueCapacity),
		runner:          runner.New("committer", cfg.logger),
	}
	if _, err := cm.runner.Run(cm.commitLoop); err != nil {
		return nil, err
	}
	return cm, nil
}

// sendCommitWaitTask sends a commit wait task to the committer.
// The commit wait task is pushed into commitWaitQ in the committer.
// The writer calls this method internally to push a commit wait task to the committer.
// If the input commit wait task is nil or empty, it panics.
func (cm *committer) sendCommitWaitTask(_ context.Context, cwt *commitWaitTask, ignoreSealing bool) (err error) {
	if cwt == nil {
		panic("log stream: committer: commit wait task is nil")
	}
	if cwt.size == 0 {
		panic("log stream: committer: commit wait task is empty")
	}

	inflight := cm.inflightCommitWait.Add(1)
	defer func() {
		if err != nil {
			inflight = cm.inflightCommitWait.Add(-1)
		}
		if ce := cm.logger.Check(zap.DebugLevel, "send committer commit wait task"); ce != nil {
			ce.Write(
				zap.Int64("inflight", inflight),
				zap.Error(err),
			)
		}
	}()

	switch cm.lse.esm.load() {
	case executorStateSealing:
		if !ignoreSealing {
			err = verrors.ErrSealed
		}
	case executorStateSealed, executorStateLearning:
		err = verrors.ErrSealed
	case executorStateClosed:
		err = verrors.ErrClosed
	}
	if err != nil {
		return err
	}

	_ = cm.commitWaitQ.push(cwt)
	return nil
}

// sendCommitTask sends a commit task to the committer.
// The commit task is pushed into commitQueue in the committer.
// The Commit RPC handler calls this method to push commit request to the committer.
func (cm *committer) sendCommitTask(ctx context.Context, ct *commitTask) (err error) {
	cm.inflightCommit.Add(1)
	defer func() {
		if err != nil {
			cm.inflightCommit.Add(-1)
		}
	}()

	// NOTE: The log stream executor in executorStateSealing state should
	// be able to accept Commit RPC since it can have logs that have been
	// written and reported but not yet committed.
	// TODO: Need to notify that the log stream is the learning state.
	switch cm.lse.esm.load() {
	case executorStateSealed, executorStateLearning:
		err = verrors.ErrSealed
	case executorStateClosed:
		err = verrors.ErrClosed
	}
	if err != nil {
		return err
	}

	if ct == nil {
		panic("log stream: committer: commit task is nil")
	}

	select {
	case cm.commitQueue <- ct:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return err
}

// commitLoop is the main loop of committer.
// It pops a commit task from the commitQueue and processes the task.
func (cm *committer) commitLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ct := <-cm.commitQueue:
			cm.commitLoopInternal(ctx, ct)
		}
	}
}

func (cm *committer) commitLoopInternal(ctx context.Context, ct *commitTask) {
	defer func() {
		ct.release()
		cm.inflightCommit.Add(-1)
	}()

	// TODO: Move these condition expressions to `internal/storagenode/logstream.(*committer).commit` method.
	commitVersion, _, _, invalid := cm.lse.lsc.reportCommitBase()
	if ct.stale(commitVersion) {
		if ce := cm.logger.Check(zap.DebugLevel, "discard a stale commit message"); ce != nil {
			ce.Write(
				zap.Any("replica", commitVersion),
				zap.Any("commit", ct.version),
			)
		}
		return
	}
	if invalid {
		// Synchronization should fix this invalid replica status
		// caused by the inconsistency between the commit context and
		// the last log entry.
		if ce := cm.logger.Check(zap.DebugLevel, "discard a commit message due to invalid replica status"); ce != nil {
			ce.Write()
		}
		return
	}

	if err := cm.commit(ctx, ct); err != nil {
		cm.logger.Error("could not commit", zap.Error(err))
		cm.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
	}
}

func (cm *committer) commit(_ context.Context, ct *commitTask) error {
	_, _, uncommittedBegin, _ := cm.lse.lsc.reportCommitBase()
	uncommittedLLSNBegin := uncommittedBegin.LLSN
	if uncommittedLLSNBegin != ct.committedLLSNBegin {
		// skip this commit
		// See #VARLOG-453
		return nil
	}

	uncommittedLLSNEnd := cm.lse.lsc.uncommittedLLSNEnd.Load()
	numUncommitted := int(uncommittedLLSNEnd - uncommittedLLSNBegin)
	numCommits := int(ct.committedGLSNEnd - ct.committedGLSNBegin)

	// When the LSE is in sealing, the number of uncommitted logs can be less than the number of
	// committed logs.
	if numUncommitted < numCommits {
		// like c.commitQ.size() < numCommits
		// skip this commit
		// NB: recovering phase?
		// MR just sends past commit messages to recovered SN that has no written logs
		return nil
	}

	// NOTE: It seems to be similar to the above condition. The actual purpose of this
	// condition is to avoid an invalid commit situation that the number of commitWaitTasks is
	// less than numCommits.
	// `numUncommitted` might be greater than or equal to the `numCommits`, but the number of
	// commitWaitTasks can be not. Since uncommittedLLSNEnd of the log stream context is
	// increased whenever each log entry is written to the storage, it doesn't represent the
	// size of commitWaitQueue. For instance, a batch of log entries could be written to the
	// storage, however, it is failed to push them into the commitWaitQueue.
	// See #VARLOG-444.
	if cm.commitWaitQ.size() < numCommits {
		return nil
	}

	// commitDirectly -> commitInternal
	commitContext := storage.CommitContext{
		Version:            ct.version,
		HighWatermark:      ct.highWatermark,
		CommittedGLSNBegin: ct.committedGLSNBegin,
		CommittedGLSNEnd:   ct.committedGLSNEnd,
		CommittedLLSNBegin: uncommittedLLSNBegin,
	}

	return cm.commitInternal(commitContext)
}

func (cm *committer) commitInternal(cc storage.CommitContext) (err error) {
	_, _, uncommittedBegin, _ := cm.lse.lsc.reportCommitBase()
	uncommttedLLSNBegin := uncommittedBegin.LLSN

	numCommits := int(cc.CommittedGLSNEnd - cc.CommittedGLSNBegin)

	// NOTE: It seems to be similar to the above condition. The actual purpose of this
	// condition is to avoid an invalid commit situation that the number of commitWaitTasks is
	// less than numCommits.
	// `numUncommitted` might be greater than or equal to the `numCommits`, but the number of
	// commitWaitTasks can be not. Since uncommittedLLSNEnd of the log stream context is
	// increased whenever each log entry is written to the storage, it doesn't represent the
	// size of commitWaitQueue. For instance, a batch of log entries could be written to the
	// storage, however, it is failed to push them into the commitWaitQueue.
	// See #VARLOG-444.
	if cm.commitWaitQ.size() < numCommits {
		return nil
	}

	startTime := time.Now()
	cb, err := cm.lse.stg.NewCommitBatch(cc)
	if err != nil {
		return err
	}

	defer func() {
		_ = cb.Close()
		if err != nil {
			return
		}
		if cm.lse.lsm == nil {
			return
		}
		cm.lse.lsm.CommitterOperationDuration.Record(context.Background(), int64(time.Since(startTime).Microseconds()))
		cm.lse.lsm.CommitterLogs.Record(context.Background(), int64(numCommits))
	}()

	iter := cm.commitWaitQ.peekIterator()
	numLogEntries := 0
	numCWTs := 0
	for numLogEntries < numCommits && iter.valid() {
		cwt := iter.task()

		// invariant: numCommits-numLogEntries >= cwt.size
		for i := range cwt.size {
			llsn := cc.CommittedLLSNBegin + types.LLSN(numLogEntries)
			glsn := cc.CommittedGLSNBegin + types.GLSN(numLogEntries)

			if uncommttedLLSNBegin+types.LLSN(numLogEntries) != llsn {
				err = errors.New("log stream: committer: llsn mismatch")
				return err
			}

			if i == 0 && cwt.awg != nil {
				// set GLSN of the first log entry in the batch
				cwt.awg.beginLSN.GLSN = glsn
			}

			err = cb.Set(llsn, glsn)
			if err != nil {
				return err
			}

			numLogEntries++
		}

		iter.next()
		numCWTs++
	}
	if numLogEntries != numCommits {
		cm.logger.Panic("commit corrupted: not matched between commit wait tasks and commit message",
			zap.Int("numCommits", numCommits),
			zap.Int("numLogEntries", numLogEntries),
			zap.Any("uncommittedBegin", uncommittedBegin),
			zap.Any("commitContext", cc),
		)
	}
	err = cb.Apply()
	if err != nil {
		return err
	}

	committedTasks := make([]*commitWaitTask, 0, numCWTs)
	for i := 0; i < numCWTs; i++ {
		// NOTE: This cwt should not be nil, because the size of commitWaitQ is inspected
		// above.
		cwt := cm.commitWaitQ.pop()
		committedTasks = append(committedTasks, cwt)
	}
	if numCommits > 0 {
		// only the first commit changes local low watermark
		cm.lse.lsc.localLWM.CompareAndSwap(varlogpb.LogSequenceNumber{}, varlogpb.LogSequenceNumber{
			LLSN: cc.CommittedLLSNBegin,
			GLSN: cc.CommittedGLSNBegin,
		})
	}
	uncommittedBegin = varlogpb.LogSequenceNumber{
		LLSN: cc.CommittedLLSNBegin + types.LLSN(numCommits),
		GLSN: cc.CommittedGLSNBegin + types.GLSN(numCommits),
	}
	cm.lse.decider.change(func() {
		cm.lse.lsc.storeReportCommitBase(cc.Version, cc.HighWatermark, uncommittedBegin, false /*invalid*/)
	})

	for _, cwt := range committedTasks {
		cwt.awg.commitDone(nil)
		cwt.release()
	}

	if len(committedTasks) > 0 {
		cm.inflightCommitWait.Add(int64(-numCWTs))
	}

	return nil
}

// drainCommitWaitQ drains the commit wait tasks in commitWaitQ.
func (cm *committer) drainCommitWaitQ(cause error) {
	if ce := cm.logger.Check(zap.DebugLevel, "draining commit wait tasks"); ce != nil {
		ce.Write(
			zap.Int64("inflight", cm.inflightCommitWait.Load()),
			zap.Error(cause),
		)
	}

	for cm.inflightCommitWait.Load() > 0 {
		cwt := cm.commitWaitQ.pop()
		if cwt == nil {
			continue
		}
		cwt.awg.commitDone(cause)
		cwt.release()
		inflight := cm.inflightCommitWait.Add(-1)
		if ce := cm.logger.Check(zap.DebugLevel, "discard a commit wait task"); ce != nil {
			ce.Write(zap.Int64("inflight", inflight))
		}
	}
}

// waitForDrainageOfCommitQueue waits for commitTasks being drained.
// The argument forceDrain should be set only if commitLoop is stopped.
func (cm *committer) waitForDrainageOfCommitQueue(forceDrain bool) {
	const tick = time.Millisecond
	timer := time.NewTimer(tick)
	defer timer.Stop()

	for cm.inflightCommit.Load() > 0 {
		if !forceDrain {
			<-timer.C
			timer.Reset(tick)
			continue
		}

		select {
		case <-timer.C:
			timer.Reset(tick)
		case ct := <-cm.commitQueue:
			ct.release()
			cm.inflightCommit.Add(-1)
		}
	}
}

func (cm *committer) stop() {
	cm.lse.esm.store(executorStateClosed)
	cm.runner.Stop()
	cm.waitForDrainageOfCommitQueue(true)
	cm.drainCommitWaitQ(verrors.ErrClosed)
}

type committerConfig struct {
	commitQueueCapacity int
	lse                 *Executor
	logger              *zap.Logger
}

func (cfg committerConfig) validate() error {
	if err := validateQueueCapacity("committer", cfg.commitQueueCapacity); err != nil {
		return fmt.Errorf("committer: %w", err)
	}
	if cfg.lse == nil {
		return fmt.Errorf("committer: %w", errExecutorIsNil)
	}
	if cfg.logger == nil {
		return fmt.Errorf("committer: %w", errLoggerIsNil)
	}
	return nil
}

var commitWaitTaskPool = sync.Pool{
	New: func() interface{} {
		return &commitWaitTask{}
	},
}

type commitWaitTask struct {
	// In the primary replica's commit operation, the size and the awg.batchLen
	// are the same. However, in the backup replica's commit operation,
	// the awg.batchLen is 0, and size indicates the number of log entries to
	// be committed. When committing log entries during the bootstrap's
	// recovery process, the awg.batchLen is 0, and the size is 1.
	awg  *appendWaitGroup
	size int // the number of log entries to be committed
}

func newCommitWaitTask(awg *appendWaitGroup, size int) *commitWaitTask {
	cwt := commitWaitTaskPool.Get().(*commitWaitTask)
	cwt.awg = awg
	cwt.size = size
	return cwt
}

func (cwt *commitWaitTask) release() {
	*cwt = commitWaitTask{}
	commitWaitTaskPool.Put(cwt)
}

var commitTaskPool = sync.Pool{
	New: func() interface{} {
		return &commitTask{}
	},
}

type commitTask struct {
	version            types.Version
	highWatermark      types.GLSN
	committedGLSNBegin types.GLSN
	committedGLSNEnd   types.GLSN
	committedLLSNBegin types.LLSN
}

func newCommitTask() *commitTask {
	return commitTaskPool.Get().(*commitTask)
}

func (ct *commitTask) release() {
	ct.version = types.InvalidVersion
	ct.highWatermark = types.InvalidGLSN
	ct.committedGLSNBegin = types.InvalidGLSN
	ct.committedGLSNEnd = types.InvalidGLSN
	ct.committedLLSNBegin = types.InvalidLLSN
	commitTaskPool.Put(ct)
}

func (ct *commitTask) stale(ver types.Version) bool {
	return ct.version <= ver
}
