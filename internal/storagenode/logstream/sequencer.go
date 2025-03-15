package logstream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/verrors"
)

type sequencer struct {
	sequencerConfig
	llsn     types.LLSN
	queue    chan *sequenceTask
	inflight atomic.Int64
	runner   *runner.Runner
}

// newSequencer creates a new sequencer.
func newSequencer(cfg sequencerConfig) (*sequencer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	sq := &sequencer{
		sequencerConfig: cfg,
		queue:           make(chan *sequenceTask, cfg.queueCapacity),
		runner:          runner.New("sequencer", cfg.logger),
	}
	if _, err := sq.runner.Run(sq.sequenceLoop); err != nil {
		return nil, err
	}
	return sq, nil
}

// send sends a sequence task to the sequencer.
// If state of the log stream executor is not appendable, it returns an error.
func (sq *sequencer) send(ctx context.Context, st *sequenceTask) (err error) {
	inflight := sq.inflight.Add(1)
	defer func() {
		if err != nil {
			inflight = sq.inflight.Add(-1)
		}
		if ce := sq.logger.Check(zap.DebugLevel, "sent seqeuencer a task"); ce != nil {
			ce.Write(
				zap.Int64("inflight", inflight),
				zap.Error(err),
			)
		}
	}()

	switch sq.lse.esm.load() {
	case executorStateSealing, executorStateSealed:
		err = verrors.ErrSealed
	case executorStateClosed:
		err = verrors.ErrClosed
	}
	if err != nil {
		return err
	}

	select {
	case sq.queue <- st:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return err
}

// sequenceLoop is the main loop of the sequencer.
// It pops a sequence task from the queue and processes the task.
func (sq *sequencer) sequenceLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case st := <-sq.queue:
			sq.sequenceLoopInternal(ctx, st)
		}
	}
}

// sequenceLoopInternal issues LLSNs to the logs, send them to writer, committer, and replicate clients.
func (sq *sequencer) sequenceLoopInternal(ctx context.Context, st *sequenceTask) {
	var startTime, operationEndTime time.Time
	defer func() {
		inflight := sq.inflight.Add(-1)
		if sq.lse.lsm == nil {
			return
		}
		sq.lse.lsm.SequencerFanoutDuration.Record(ctx, time.Since(operationEndTime).Microseconds())
		sq.lse.lsm.SequencerOperationDuration.Record(ctx, int64(operationEndTime.Sub(startTime).Microseconds()))
		sq.lse.lsm.ReplicateClientInflightOperations.Store(inflight)
	}()

	startTime = time.Now()

	beginLLSN := sq.llsn + 1
	sq.llsn += types.LLSN(len(st.dataBatch))
	if ce := sq.logger.Check(zap.DebugLevel, "sequencer: issued llsn"); ce != nil {
		ce.Write(zap.Uint64("first", uint64(beginLLSN)), zap.Uint64("last", uint64(sq.llsn)))
	}
	st.awg.setBeginLLSN(beginLLSN)
	for replicaIdx := range st.rts.tasks {
		st.rts.tasks[replicaIdx].beginLLSN = beginLLSN
	}

	operationEndTime = time.Now()

	// NOTE: cwts and rts must be set before sending st to writer, since st can be released after sending.
	cwt := st.cwt
	rts := st.rts

	// NOTE: If sending to the writer is ahead of sending to the committer,
	// it is very subtle when sending tasks to the committer fails but
	// sending tasks to the writer succeeds.
	// - If tasks are sent to the writer successfully, they may be
	// reflected by reports.
	// - Then, if commitWaitTasks is not sent to the writer, the committer
	// has no tasks to commit.
	// - In addition, the Append RPC handler can wait indefinitely to wait
	// for the commit that the committer won't process.
	//
	// To avoid this problem, sending tasks to committer must be happened
	// before sending tasks to writer. It prevents the above subtle case.
	//
	// send to committer
	if err := sq.lse.cm.sendCommitWaitTask(ctx, cwt, false /*ignoreSealing*/); err != nil {
		sq.logger.Error("could not send to committer", zap.Error(err))
		sq.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
		st.wwg.done(err)
		cwt.release()
		releaseReplicateTasks(rts.tasks)
		releaseReplicateTaskSlice(rts)
		st.release()
		return
	}

	// send to writer
	if err := sq.lse.wr.send(ctx, st); err != nil {
		sq.logger.Error("could not send to writer", zap.Error(err))
		sq.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
		st.wwg.done(err)
		releaseReplicateTasks(rts.tasks)
		releaseReplicateTaskSlice(rts)
		st.release()
		return
	}

	// send to replicator
	ridx := 0
	for ridx < len(rts.tasks) {
		err := sq.lse.rcs.clients[ridx].send(ctx, rts.tasks[ridx])
		if err != nil {
			sq.logger.Error("could not send to replicate client", zap.Error(err))
			sq.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
			break
		}
		ridx++
	}
	releaseReplicateTasks(rts.tasks[ridx:])
	releaseReplicateTaskSlice(rts)
}

// waitForDrainage waits for draining of queue in the sequencer.
// If the argument forceDrain is true, it drops sequence tasks from the queue and uses the argument cause as a reason for the write error.
// If the argument forceDrain is false, it just waits for the queue is empty. To clear the queue, writeLoop should be running.
func (sq *sequencer) waitForDrainage(cause error, forceDrain bool) {
	const tick = time.Millisecond
	timer := time.NewTimer(tick)
	defer timer.Stop()

	if ce := sq.logger.Check(zap.DebugLevel, "draining sequencer tasks"); ce != nil {
		ce.Write(
			zap.Int64("inflight", sq.inflight.Load()),
			zap.Error(cause),
		)
	}

	for sq.inflight.Load() > 0 {
		if !forceDrain {
			<-timer.C
			timer.Reset(tick)
			continue
		}

		select {
		case <-timer.C:
			timer.Reset(tick)
		case st := <-sq.queue:
			st.awg.writeDone(cause)
			st.awg.commitDone(nil)
			sq.inflight.Add(-1)
		}
	}
}

// stop terminates sequencer.
// The terminated sequencer cannot be used.
func (sq *sequencer) stop() {
	sq.lse.esm.store(executorStateClosed)
	sq.runner.Stop()
	sq.waitForDrainage(verrors.ErrClosed, true)
}

type sequencerConfig struct {
	queueCapacity int
	lse           *Executor
	logger        *zap.Logger
}

func (cfg sequencerConfig) validate() error {
	if err := validateQueueCapacity("sequencer", cfg.queueCapacity); err != nil {
		return fmt.Errorf("sequencer: %w", err)
	}
	if cfg.lse == nil {
		return fmt.Errorf("sequencer: %w", errExecutorIsNil)
	}
	if cfg.logger == nil {
		return fmt.Errorf("sequencer: %w", errLoggerIsNil)
	}
	return nil
}

var sequenceTaskPool = sync.Pool{
	New: func() interface{} {
		return &sequenceTask{}
	},
}

type sequenceTask struct {
	wwg       *writeWaitGroup
	awg       *appendWaitGroup
	dataBatch [][]byte
	cwt       *commitWaitTask
	rts       *replicateTaskSlice
}

func newSequenceTask() *sequenceTask {
	st := sequenceTaskPool.Get().(*sequenceTask)
	return st
}

func (st *sequenceTask) release() {
	st.wwg = nil
	st.awg = nil
	st.dataBatch = nil
	st.cwt = nil
	st.rts = nil
	sequenceTaskPool.Put(st)
}
