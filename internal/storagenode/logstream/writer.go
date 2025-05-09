package logstream

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/verrors"
)

type writer struct {
	writerConfig
	queue    chan *sequenceTask
	inflight atomic.Int64
	runner   *runner.Runner
}

// newWriter creates a new writer.
func newWriter(cfg writerConfig) (*writer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	w := &writer{
		writerConfig: cfg,
		queue:        make(chan *sequenceTask, cfg.queueCapacity),
		runner:       runner.New("writer", zap.NewNop()),
	}
	if _, err := w.runner.Run(w.writeLoop); err != nil {
		return nil, err
	}
	return w, nil
}

// send sends a sequenceTask to the queue.
func (w *writer) send(ctx context.Context, st *sequenceTask) (err error) {
	inflight := w.inflight.Add(1)
	defer func() {
		if err != nil {
			inflight = w.inflight.Add(-1)
		}
		if ce := w.logger.Check(zap.DebugLevel, "sent writer a task"); ce != nil {
			ce.Write(
				zap.Int64("inflight", inflight),
				zap.Error(err),
			)
		}
	}()

	switch w.lse.esm.load() {
	case executorStateSealing, executorStateSealed, executorStateLearning:
		err = verrors.ErrSealed
	case executorStateClosed:
		err = verrors.ErrClosed
	}
	if err != nil {
		return err
	}

	select {
	case w.queue <- st:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return err
}

// writeLoop is the main loop of the writer.
// It pops a sequence task from the queue and processes the task.
func (w *writer) writeLoop(ctx context.Context) {
	maxTasks := w.lse.maxWriteTaskBatchLength
	sts := make([]*sequenceTask, 0, maxTasks)

	for {
		select {
		case <-ctx.Done():
			return
		case st := <-w.queue:
			sts = append(sts, st)
			remains := min(maxTasks-1, len(w.queue))
			for range remains {
				st := <-w.queue
				sts = append(sts, st)
			}
			w.writeLoopInternal(ctx, sts)
			sts = sts[:0]
		}
	}
}

// writeLoopInternal stores a batch of writes to the storage and modifies uncommittedLLSNEnd of the log stream which presents the next expected LLSN to be written.
func (w *writer) writeLoopInternal(ctx context.Context, sts []*sequenceTask) {
	startTime := time.Now()
	var err error
	wb := w.lse.stg.NewWriteBatch()
	defer func() {
		if err != nil {
			w.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
		}
		_ = wb.Close()
		for _, st := range sts {
			st.wwg.done(err)
			st.release()
		}
		inflight := w.inflight.Add(-int64(len(sts)))
		if w.lse.lsm == nil {
			return
		}
		w.lse.lsm.WriterOperationDuration.Record(ctx, time.Since(startTime).Microseconds())
		w.lse.lsm.WriterInflightOperations.Store(inflight)
	}()

	oldLLSN := sts[0].awg.beginLSN.LLSN
	newLLSN := sts[len(sts)-1].awg.beginLSN.LLSN + types.LLSN(len(sts[len(sts)-1].dataBatch))
	if uncommittedLLSNEnd := w.lse.lsc.uncommittedLLSNEnd.Load(); uncommittedLLSNEnd != oldLLSN {
		err = fmt.Errorf("unexpected LLSN: uncommittedLLSNEnd=%d, oldLLSN=%d, newLLSN=%d", uncommittedLLSNEnd, oldLLSN, newLLSN)
		w.logger.Error("try to write log entries at unexpected LLSN in the primary replica",
			zap.Uint64("uncommittedLLSNEnd", uint64(uncommittedLLSNEnd)),
			zap.Uint64("startOfBatch", uint64(oldLLSN)),
			zap.Uint64("endOfBatch", uint64(newLLSN-1)),
			zap.Error(err),
		)
		return
	}

	var offset types.LLSN
	for _, st := range sts {
		for i := range st.dataBatch {
			llsn := oldLLSN + offset
			err = wb.Set(llsn, st.dataBatch[i])
			if err != nil {
				w.logger.Error("could not set data to batch", zap.Error(err))
				return
			}
			offset++
		}
	}

	err = wb.Apply()
	if err != nil {
		w.logger.Error("could not apply data", zap.Error(err))
		return
	}

	if !w.lse.lsc.uncommittedLLSNEnd.CompareAndSwap(oldLLSN, newLLSN) {
		// NOTE: If this panic occurs, it may be very subtle.
		// We can't simply guarantee whether unexpected LLSNs are
		// already discarded by sealing or those are new data but
		// uncommittedLLSNEnd is wrong.
		// As a simple solution, we can use epoch issued by the
		// metadata repository and incremented whenever unsealing to
		// decide if the logs are stale or not.
		uncommittedLLSNEnd := w.lse.lsc.uncommittedLLSNEnd.Load()
		err = fmt.Errorf("unexpected LLSN: uncommittedLLSNEnd=%d, oldLLSN=%d, newLLSN=%d", uncommittedLLSNEnd, oldLLSN, newLLSN)
		w.logger.DPanic(
			"uncommittedLLSNEnd swap failure: unexpected batch was written into the primary replica",
			zap.Uint64("uncommittedLLSNEnd", uint64(uncommittedLLSNEnd)),
			zap.Uint64("startOfBatch", uint64(oldLLSN)),
			zap.Uint64("endOfBatch", uint64(newLLSN-1)),
			zap.Error(err),
		)
	}
}

// waitForDrainage waits for writeTasks being drained.
// The argument forceDrain should be set only if writeLoop is stopped.
func (w *writer) waitForDrainage(cause error, forceDrain bool) {
	const tick = time.Millisecond
	timer := time.NewTimer(tick)
	defer timer.Stop()

	if ce := w.logger.Check(zap.DebugLevel, "draining writer tasks"); ce != nil {
		ce.Write(
			zap.Int64("inflight", w.inflight.Load()),
			zap.Error(cause),
		)
	}

	for w.inflight.Load() > 0 {
		if !forceDrain {
			<-timer.C
			timer.Reset(tick)
			continue
		}

		select {
		case <-timer.C:
			timer.Reset(tick)
		case st := <-w.queue:
			st.awg.writeDone(cause)
			st.release()
			w.inflight.Add(-1)
		}
	}
}

// stop terminates the writer.
// The terminated writer cannot be used.
func (w *writer) stop() {
	w.lse.esm.store(executorStateClosed)
	w.runner.Stop()
	w.waitForDrainage(verrors.ErrClosed, true)
}

type writerConfig struct {
	queueCapacity int
	lse           *Executor
	logger        *zap.Logger
}

func (cfg writerConfig) validate() error {
	if err := validateQueueCapacity("writer", cfg.queueCapacity); err != nil {
		return fmt.Errorf("writer: %w", err)
	}
	if cfg.lse == nil {
		return fmt.Errorf("writer: %w", errExecutorIsNil)
	}
	if cfg.logger == nil {
		return fmt.Errorf("writer: %w", errLoggerIsNil)
	}
	return nil
}
