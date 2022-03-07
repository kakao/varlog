package logstream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

type sequencer struct {
	sequencerConfig
	llsn     types.LLSN
	queue    chan *sequenceTask
	inflight int64
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
	atomic.AddInt64(&sq.inflight, 1)
	defer func() {
		if err != nil {
			atomic.AddInt64(&sq.inflight, -1)
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
		inflight := atomic.AddInt64(&sq.inflight, -1)
		if sq.lse.lsm == nil {
			return
		}
		atomic.AddInt64(&sq.lse.lsm.SequencerFanoutDuration, time.Since(operationEndTime).Microseconds())
		atomic.AddInt64(&sq.lse.lsm.SequencerOperationDuration, operationEndTime.Sub(startTime).Microseconds())
		atomic.AddInt64(&sq.lse.lsm.SequencerOperations, 1)
		atomic.StoreInt64(&sq.lse.lsm.ReplicateClientInflightOperations, inflight)
	}()

	startTime = time.Now()

	for dataIdx := 0; dataIdx < len(st.awgs); dataIdx++ {
		sq.llsn++
		st.awgs[dataIdx].setLLSN(sq.llsn)
		for replicaIdx := 0; replicaIdx < len(st.rts); replicaIdx++ {
			// NOTE: Use "append" since the length of st.rts is not enough to use index. Its capacity is enough because it is created to be reused.
			st.rts[replicaIdx].llsnList = append(st.rts[replicaIdx].llsnList, sq.llsn)
		}
		if err := st.wb.Set(sq.llsn, st.dataBatch[dataIdx]); err != nil {
			// TODO: handle error
		}
		// st.dwb.SetLLSN(dataIdx, sq.llsn)
	}

	operationEndTime = time.Now()

	// NOTE: cwts and rts must be set before sending st to writer, since st can be released after sending.
	cwts := st.cwts
	rts := st.rts

	// send to writer
	if err := sq.lse.wr.send(ctx, st); err != nil {
		sq.logger.Error("could not send to writer", zap.Error(err))
		sq.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
		st.wwg.done(err)
		// _ = st.dwb.Close()
		_ = st.wb.Close()
		releaseCommitWaitTaskList(st.cwts)
		releaseReplicateTasks(st.rts)
		releaseReplicateTaskSlice(rts)
		st.release()
		return
	}

	// send to committer
	if err := sq.lse.cm.sendCommitWaitTask(ctx, cwts); err != nil {
		sq.logger.Error("could not send to committer", zap.Error(err))
		sq.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
		releaseCommitWaitTaskList(cwts)
		releaseReplicateTasks(rts)
		releaseReplicateTaskSlice(rts)
		return
	}

	// send to replicator
	ridx := 0
	for ridx < len(rts) {
		err := sq.lse.rcs.clients[ridx].send(ctx, rts[ridx])
		if err != nil {
			sq.logger.Error("could not send to replicate client", zap.Error(err))
			sq.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
			break
		}
		ridx++
	}
	releaseReplicateTasks(rts[ridx:])
	releaseReplicateTaskSlice(rts)
}

// waitForDrainage waits for draining of queue in the sequencer.
// If the argument forceDrain is true, it drops sequence tasks from the queue and uses the argument cause as a reason for the write error.
// If the argument forceDrain is false, it just waits for the queue is empty. To clear the queue, writeLoop should be running.
func (sq *sequencer) waitForDrainage(cause error, forceDrain bool) {
	const tick = time.Millisecond
	timer := time.NewTimer(tick)
	defer timer.Stop()

	for atomic.LoadInt64(&sq.inflight) > 0 || len(sq.queue) > 0 {
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
		case st := <-sq.queue:
			for i := 0; i < len(st.awgs); i++ {
				st.awgs[i].writeDone(cause)
				st.awgs[i].commitDone(nil)
			}
			atomic.AddInt64(&sq.inflight, -1)
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
	wwg  *writeWaitGroup
	awgs []*appendWaitGroup
	// dwb  *storage.DeferredWriteBatch
	wb        *storage.WriteBatch
	dataBatch [][]byte
	cwts      *listQueue
	rts       []*replicateTask
}

func newSequenceTask() *sequenceTask {
	st := sequenceTaskPool.Get().(*sequenceTask)
	return st
}

//func newSequenceTask(wwg *writeWaitGroup, dwb *storage.DeferredWriteBatch, awgs []*appendWaitGroup, cwts *listQueue, rts []*replicateTask) *sequenceTask {
//	st := sequenceTaskPool.Get().(*sequenceTask)
//	st.wwg = wwg
//	st.awgs = awgs
//	st.dwb = dwb
//	st.cwts = cwts
//	st.rts = rts
//	return st
//}

func (st *sequenceTask) release() {
	st.wwg = nil
	st.awgs = nil
	// st.dwb = nil
	st.wb = nil
	st.dataBatch = nil
	st.cwts = nil
	st.rts = nil
	sequenceTaskPool.Put(st)
}
