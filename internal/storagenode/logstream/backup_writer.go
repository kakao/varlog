package logstream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/verrors"
)

type backupWriter struct {
	backupWriterConfig
	queue    chan *backupWriteTask
	inflight int64
	runner   *runner.Runner
}

func newBackupWriter(cfg backupWriterConfig) (*backupWriter, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	sq := &backupWriter{
		backupWriterConfig: cfg,
		queue:              make(chan *backupWriteTask, cfg.queueCapacity),
		runner:             runner.New("backup writer", cfg.logger),
	}
	if _, err := sq.runner.Run(sq.writeLoop); err != nil {
		return nil, err
	}
	return sq, nil
}

func (bw *backupWriter) send(ctx context.Context, bwt *backupWriteTask) (err error) {
	atomic.AddInt64(&bw.inflight, 1)
	defer func() {
		if err != nil {
			atomic.AddInt64(&bw.inflight, -1)
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
	case bw.queue <- bwt:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return err
}

func (bw *backupWriter) writeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case bwt := <-bw.queue:
			bw.writeLoopInternal(ctx, bwt)
		}
	}
}

func (bw *backupWriter) writeLoopInternal(_ context.Context, bwt *backupWriteTask) {
	startTime := time.Now()
	wb, oldLLSN, newLLSN := bwt.wb, bwt.oldLLSN, bwt.newLLSN
	defer func() {
		_ = wb.Close()
		bwt.release()
		atomic.AddInt64(&bw.inflight, -1)
		if bw.lse.lsm == nil {
			return
		}
		atomic.AddInt64(&bw.lse.lsm.WriterOperationDuration, time.Since(startTime).Microseconds())
		atomic.AddInt64(&bw.lse.lsm.WriterOperations, 1)

	}()

	err := wb.Apply()
	if err != nil {
		bw.logger.Error("could not apply backup data", zap.Error(err))
		bw.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
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
		bw.logger.Panic(
			"uncommittedLLSNEnd swap failure",
			zap.Uint64("uncommittedLLSNEnd", uint64(bw.lse.lsc.uncommittedLLSNEnd.Load())),
			zap.Uint64("cas_old", uint64(oldLLSN)),
			zap.Uint64("cas_new", uint64(newLLSN)),
		)
	}
}

func (bw *backupWriter) waitForDrainage(forceDrain bool) {
	const tick = time.Millisecond
	timer := time.NewTimer(tick)
	defer timer.Stop()

	for atomic.LoadInt64(&bw.inflight) > 0 || len(bw.queue) > 0 {
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
		case bt := <-bw.queue:
			bt.release()
			atomic.AddInt64(&bw.inflight, -1)
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

var backupWriteTaskPool = sync.Pool{
	New: func() interface{} {
		return &backupWriteTask{}
	},
}

type backupWriteTask struct {
	wb      *storage.WriteBatch
	oldLLSN types.LLSN
	newLLSN types.LLSN
}

func newBackupWriteTask(wb *storage.WriteBatch, oldLLSN, newLLSN types.LLSN) *backupWriteTask {
	bwt := backupWriteTaskPool.Get().(*backupWriteTask)
	bwt.wb = wb
	bwt.oldLLSN = oldLLSN
	bwt.newLLSN = newLLSN
	return bwt
}

func (bwt *backupWriteTask) release() {
	bwt.wb = nil
	bwt.oldLLSN = types.InvalidLLSN
	bwt.newLLSN = types.InvalidLLSN
	backupWriteTaskPool.Put(bwt)
}
