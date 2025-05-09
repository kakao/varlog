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

type backupWriter struct {
	backupWriterConfig
	queue    chan *ReplicationTask
	inflight atomic.Int64
	runner   *runner.Runner
}

func newBackupWriter(cfg backupWriterConfig) (*backupWriter, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	sq := &backupWriter{
		backupWriterConfig: cfg,
		queue:              make(chan *ReplicationTask, cfg.queueCapacity),
		runner:             runner.New("backup writer", cfg.logger),
	}
	if _, err := sq.runner.Run(sq.writeLoop); err != nil {
		return nil, err
	}
	return sq, nil
}

func (bw *backupWriter) send(ctx context.Context, rt *ReplicationTask) (err error) {
	bw.inflight.Add(1)
	defer func() {
		if err != nil {
			bw.inflight.Add(-1)
		}
	}()

	switch bw.lse.esm.load() {
	case executorStateSealing, executorStateSealed, executorStateLearning:
		err = verrors.ErrSealed
	case executorStateClosed:
		err = verrors.ErrClosed
	}
	if err != nil {
		return err
	}

	select {
	case bw.queue <- rt:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return err
}

func (bw *backupWriter) writeLoop(ctx context.Context) {
	maxTasks := bw.lse.maxWriteTaskBatchLength
	rts := make([]*ReplicationTask, 0, maxTasks)

	for {
		select {
		case <-ctx.Done():
			return
		case rt := <-bw.queue:
			rts = append(rts, rt)
			remains := min(maxTasks-1, len(bw.queue))
			for range remains {
				rst := <-bw.queue
				rts = append(rts, rst)
			}
			bw.writeLoopInternal(ctx, rts)
			rts = rts[:0]
		}
	}
}

func (bw *backupWriter) writeLoopInternal(ctx context.Context, rts []*ReplicationTask) {
	startTime := time.Now()
	var err error

	dataBytes := int64(0)
	wb := bw.lse.stg.NewWriteBatch()
	defer func() {
		if err != nil {
			bw.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
		}
		_ = wb.Close()
		for _, rt := range rts {
			rt.Release()
		}
		bw.inflight.Add(-int64(len(rts)))
		if bw.lse.lsm == nil {
			return
		}
		bw.lse.lsm.WriterOperationDuration.Record(ctx, time.Since(startTime).Microseconds())
	}()

	oldLLSN := rts[0].Req.BeginLLSN
	newLLSN := rts[len(rts)-1].Req.BeginLLSN + types.LLSN(len(rts[len(rts)-1].Req.Data))

	if uncommittedLLSNEnd := bw.lse.lsc.uncommittedLLSNEnd.Load(); uncommittedLLSNEnd != oldLLSN {
		err = fmt.Errorf("unexpected LLSN: uncommittedLLSNEnd=%d, oldLLSN=%d, newLLSN=%d", uncommittedLLSNEnd, oldLLSN, newLLSN)
		bw.logger.Error("try to write log entries at unexpected LLSN in the backup replica",
			zap.Uint64("uncommittedLLSNEnd", uint64(uncommittedLLSNEnd)),
			zap.Uint64("startOfBatch", uint64(oldLLSN)),
			zap.Uint64("endOfBatch", uint64(newLLSN-1)),
			zap.Error(err),
		)
		return
	}

	for _, rt := range rts {
		beginLLSN := rt.Req.BeginLLSN
		for i := range rt.Req.Data {
			llsn := beginLLSN + types.LLSN(i)
			err = wb.Set(llsn, rt.Req.Data[i])
			if err != nil {
				bw.logger.Error("could not set data to batch", zap.Error(err))
				return
			}
			dataBytes += int64(len(rt.Req.Data[i]))
		}
	}

	err = wb.Apply()
	if err != nil {
		bw.logger.Error("could not apply backup data", zap.Error(err))
		return
	}

	if !bw.lse.lsc.uncommittedLLSNEnd.CompareAndSwap(oldLLSN, newLLSN) {
		// NOTE: If this panic occurs, it may be very subtle.
		// We can't simply guarantee whether unexpected LLSNs are
		// already discarded by sealing or those are new data but
		// uncommittedLLSNEnd is wrong.
		// As a simple solution, we can use epoch issued by the
		// metadata repository and incremented whenever unsealing to
		// decide if the logs are stale or not.
		uncommittedLLSNEnd := bw.lse.lsc.uncommittedLLSNEnd.Load()
		err = fmt.Errorf("unexpected LLSN: uncommittedLLSNEnd=%d, oldLLSN=%d, newLLSN=%d", uncommittedLLSNEnd, oldLLSN, newLLSN)
		bw.logger.DPanic(
			"uncommittedLLSNEnd swap failure: unexpected batch was written into the backup replica",
			zap.Uint64("uncommittedLLSNEnd", uint64(uncommittedLLSNEnd)),
			zap.Uint64("startOfBatch", uint64(oldLLSN)),
			zap.Uint64("endOfBatch", uint64(newLLSN-1)),
			zap.Error(err),
		)
	}
}

func (bw *backupWriter) waitForDrainage(forceDrain bool) {
	const tick = time.Millisecond
	timer := time.NewTimer(tick)
	defer timer.Stop()

	for bw.inflight.Load() > 0 {
		if !forceDrain {
			<-timer.C
			timer.Reset(tick)
			continue
		}

		select {
		case <-timer.C:
			timer.Reset(tick)
		case rt := <-bw.queue:
			rt.Release()
			bw.inflight.Add(-1)
		}
	}
}

// stop terminates backupWriter.
// The terminated backupWriter cannot be used.
func (bw *backupWriter) stop() {
	bw.lse.esm.store(executorStateClosed)
	bw.runner.Stop()
	bw.waitForDrainage(true)
}

type backupWriterConfig struct {
	queueCapacity int
	lse           *Executor
	logger        *zap.Logger
}

func (cfg backupWriterConfig) validate() error {
	if err := validateQueueCapacity("backup writer", cfg.queueCapacity); err != nil {
		return fmt.Errorf("backup writer: %w", err)
	}
	if cfg.lse == nil {
		return fmt.Errorf("backup writer: %w", errExecutorIsNil)
	}
	if cfg.logger == nil {
		return fmt.Errorf("backup writer: %w", errLoggerIsNil)
	}
	return nil
}
