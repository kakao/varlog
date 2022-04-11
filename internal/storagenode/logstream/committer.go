package logstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type committer struct {
	committerConfig

	commitWaitQ        *commitWaitQueue
	inflightCommitWait int64

	registerQueue     chan []*commitWaitTask
	intflightRegister int64

	commitQueue    chan *commitTask
	inflightCommit int64

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

// sendCommitWaitTask sends a list of commit wait tasks to the committer.
// The commit wait task is pushed into commitWaitQ in the committer.
// The writer calls this method internally to push commit wait tasks to the committer.
// If the input list of commit wait tasks are nil or empty, it panics.
func (cm *committer) sendCommitWaitTask(_ context.Context, cwts *listQueue) (err error) {
	if cwts == nil {
		panic("log stream: committer: commit wait task list is nil")
	}
	cnt := cwts.Len()
	if cnt == 0 {
		panic("log stream: committer: commit wait task list is empty")
	}

	atomic.AddInt64(&cm.inflightCommitWait, int64(cnt))
	defer func() {
		if err != nil {
			atomic.AddInt64(&cm.inflightCommitWait, -int64(cnt))
		}
	}()

	switch cm.lse.esm.load() {
	case executorStateSealing, executorStateSealed, executorStateLearning:
		err = verrors.ErrSealed
	case executorStateClosed:
		err = verrors.ErrClosed
	}
	if err != nil {
		return err
	}

	_ = cm.commitWaitQ.pushList(cwts)
	return nil
}

// sendCommitTask sends a commit task to the committer.
// The commit task is pushed into commitQueue in the committer.
// The Commit RPC handler calls this method to push commit request to the committer.
func (cm *committer) sendCommitTask(ctx context.Context, ct *commitTask) (err error) {
	atomic.AddInt64(&cm.inflightCommit, 1)
	defer func() {
		if err != nil {
			atomic.AddInt64(&cm.inflightCommit, -1)
		}
	}()

	switch cm.lse.esm.load() {
	case executorStateSealing, executorStateSealed, executorStateLearning:
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
		atomic.AddInt64(&cm.inflightCommit, -1)
	}()
	commitVersion, _, _ := cm.lse.lsc.reportCommitBase()
	if ct.stale(commitVersion) {
		return
	}
	if err := cm.commit(ctx, ct); err != nil {
		cm.logger.Error("could not commit", zap.Error(err))
		cm.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
	}
}

func (cm *committer) commit(_ context.Context, ct *commitTask) error {
	_, _, uncommittedLLSNBegin := cm.lse.lsc.reportCommitBase()
	if uncommittedLLSNBegin != ct.committedLLSNBegin {
		// skip this commit
		// See #VARLOG-453 (https://jira.daumkakao.com/browse/VARLOG-453).
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
	// See [#VARLOG-444](https://jira.daumkakao.com/browse/VARLOG-444).
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

	return cm.commitInternal(commitContext, true)
}

func (cm *committer) commitInternal(cc storage.CommitContext, requireCommitWaitTasks bool) (err error) {
	_, _, uncommittedLLSNBegin := cm.lse.lsc.reportCommitBase()
	numCommits := int(cc.CommittedGLSNEnd - cc.CommittedGLSNBegin)

	// NOTE: It seems to be similar to the above condition. The actual purpose of this
	// condition is to avoid an invalid commit situation that the number of commitWaitTasks is
	// less than numCommits.
	// `numUncommitted` might be greater than or equal to the `numCommits`, but the number of
	// commitWaitTasks can be not. Since uncommittedLLSNEnd of the log stream context is
	// increased whenever each log entry is written to the storage, it doesn't represent the
	// size of commitWaitQueue. For instance, a batch of log entries could be written to the
	// storage, however, it is failed to push them into the commitWaitQueue.
	// See [#VARLOG-444](https://jira.daumkakao.com/browse/VARLOG-444).
	if requireCommitWaitTasks && cm.commitWaitQ.size() < numCommits {
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
		atomic.AddInt64(&cm.lse.lsm.CommitterOperationDuration, time.Since(startTime).Microseconds())
		atomic.AddInt64(&cm.lse.lsm.CommitterOperations, 1)
		atomic.AddInt64(&cm.lse.lsm.CommitterLogs, int64(numCommits))
	}()

	iter := cm.commitWaitQ.peekIterator()
	for i := 0; i < numCommits; i++ {
		llsn := cc.CommittedLLSNBegin + types.LLSN(i)
		glsn := cc.CommittedGLSNBegin + types.GLSN(i)

		if uncommittedLLSNBegin+types.LLSN(i) != llsn {
			err = errors.New("log stream: committer: llsn mismatch")
			return err
		}

		// If requireCommitWaitTasks is true, since the number of tasks in commitWaitQ is
		// inspected above, cwt must exist.
		// If cwt is null, it means that there is no task in commitWaitQ anymore. When this
		// method is executed by SyncReplicate, it is okay for cwt not to exist.
		cwt := iter.task()
		if cwt != nil {
			cwt.awg.setGLSN(glsn)
		}

		err = cb.Set(llsn, glsn)
		if err != nil {
			return err
		}

		iter.next()
	}
	err = cb.Apply()
	if err != nil {
		return err
	}

	committedTasks := make([]*commitWaitTask, 0, numCommits)
	for i := 0; i < numCommits; i++ {
		// NOTE: This cwt should not be nil, because the size of commitWaitQ is inspected
		// above.
		cwt := cm.commitWaitQ.pop()
		if cwt != nil {
			// FIXME (jun): If sync replication is occurred, cwt may be nil.
			// It means that above codes tried to pop from empty commitWaitQ.
			// It is very difficult to detect bug, so it should be re-organized.
			committedTasks = append(committedTasks, cwt)
		}
	}

	if numCommits > 0 {
		// only the first commit changes local low watermark
		localLWM := varlogpb.LogEntryMeta{
			LLSN: cc.CommittedLLSNBegin,
			GLSN: cc.CommittedGLSNBegin,
		}
		cm.lse.lsc.localWatermarks.low.CompareAndSwap(varlogpb.InvalidLogEntryMeta(), localLWM)

		localHWM := varlogpb.LogEntryMeta{
			LLSN: cc.CommittedLLSNBegin + types.LLSN(numCommits) - 1,
			GLSN: cc.CommittedGLSNBegin + types.GLSN(numCommits) - 1,
		}
		cm.lse.lsc.setLocalHighWatermark(localHWM)
	}
	uncommittedLLSNBegin += types.LLSN(numCommits)

	cm.lse.decider.change(func() {
		cm.lse.lsc.storeReportCommitBase(cc.Version, cc.HighWatermark, uncommittedLLSNBegin)
	})

	for _, cwt := range committedTasks {
		cwt.awg.commitDone(nil)
		cwt.release()
	}

	// NOTE: When sync replication is occurred, it can be zero.
	if len(committedTasks) > 0 {
		atomic.AddInt64(&cm.inflightCommitWait, int64(-numCommits))
	}

	return nil
}

// drainCommitWaitQ drains the commit wait tasks in commitWaitQ.
func (cm *committer) drainCommitWaitQ(cause error) {
	cm.logger.Debug("draining commit wait tasks",
		zap.Int64("inflight", atomic.LoadInt64(&cm.inflightCommitWait)),
		zap.Error(cause),
	)

	for atomic.LoadInt64(&cm.inflightCommitWait) > 0 {
		cwt := cm.commitWaitQ.pop()
		if cwt == nil {
			continue
		}
		cwt.awg.commitDone(cause)
		cwt.release()
		inflight := atomic.AddInt64(&cm.inflightCommitWait, -1)
		cm.logger.Debug("discard a commit wait task", zap.Int64("inflight", inflight))
	}
}

// waitForDrainageOfCommitQueue waits for commitTasks being drained.
// The argument forceDrain should be set only if commitLoop is stopped.
func (cm *committer) waitForDrainageOfCommitQueue(forceDrain bool) {
	const tick = time.Millisecond
	timer := time.NewTimer(tick)
	defer timer.Stop()

	for atomic.LoadInt64(&cm.inflightCommit) > 0 {
		if !forceDrain {
			select {
			case <-timer.C:
				timer.Reset(tick)
			}
			continue
		}

		select {
		case <-timer.C:
			timer.Reset(tick)
		case ct := <-cm.commitQueue:
			ct.release()
			atomic.AddInt64(&cm.inflightCommit, -1)
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
	awg *appendWaitGroup
}

func newCommitWaitTask(awg *appendWaitGroup) *commitWaitTask {
	// Backup replica has no awg.
	cwt := commitWaitTaskPool.Get().(*commitWaitTask)
	cwt.awg = awg
	return cwt
}

func (cwt *commitWaitTask) release() {
	cwt.awg = nil
	commitWaitTaskPool.Put(cwt)
}

func releaseCommitWaitTaskList(cwts *listQueue) {
	cnt := cwts.Len()
	for i := 0; i < cnt; i++ {
		cwt := cwts.RemoveBack().(*commitWaitTask)
		cwt.release()
	}
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
