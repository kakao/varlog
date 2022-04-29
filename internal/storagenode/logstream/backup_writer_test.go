package logstream

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func TestBackupWriter_InvalidConfig(t *testing.T) {
	_, err := newBackupWriter(backupWriterConfig{
		queueCapacity: minQueueCapacity - 1,
		lse:           &Executor{},
		logger:        zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newBackupWriter(backupWriterConfig{
		queueCapacity: maxQueueCapacity + 1,
		lse:           &Executor{},
		logger:        zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newBackupWriter(backupWriterConfig{
		logger: zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newBackupWriter(backupWriterConfig{
		lse: &Executor{},
	})
	assert.Error(t, err)
}

func TestBackupWriter_ShouldNotAcceptTasksWhileNotAppendable(t *testing.T) {
	lse := &Executor{
		esm: newExecutorStateManager(executorStateSealing),
	}
	bw := &backupWriter{}
	bw.lse = lse
	bw.logger = zap.NewNop()

	lse.esm.store(executorStateSealing)
	err := bw.send(context.Background(), &backupWriteTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateSealed)
	err = bw.send(context.Background(), &backupWriteTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateClosed)
	err = bw.send(context.Background(), &backupWriteTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateAppendable)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = bw.send(canceledCtx, &backupWriteTask{})
	assert.Error(t, err)
}

func TestBackupWriter_Drain(t *testing.T) {
	const numTasks = 10

	stg := storage.TestNewStorage(t)
	defer func() {
		assert.NoError(t, stg.Close())
	}()

	lse := &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}

	bw := &backupWriter{}
	bw.lse = lse
	bw.queue = make(chan *backupWriteTask, numTasks)
	bw.logger = zap.NewNop()

	for i := 0; i < numTasks; i++ {
		wb := stg.NewWriteBatch()
		bwt := newBackupWriteTask(wb, types.LLSN(i+1), types.LLSN(i+2))
		err := bw.send(context.Background(), bwt)
		assert.NoError(t, err)
	}

	assert.EqualValues(t, numTasks, atomic.LoadInt64(&bw.inflight))
	assert.Len(t, bw.queue, numTasks)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bw.waitForDrainage(false)
	}()
	runtime.Gosched()
	bw.waitForDrainage(true)
	wg.Wait()

	assert.Zero(t, atomic.LoadInt64(&bw.inflight))
	assert.Empty(t, bw.queue)
}

func TestBackupWriter_UnexpectedLLSN(t *testing.T) {
	stg := storage.TestNewStorage(t)
	defer func() {
		assert.NoError(t, stg.Close())
	}()

	// LLSN to be expected to come is 2.
	const uncommittedLLSNEnd = types.LLSN(2)

	lse := &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
		lsc: newLogStreamContext(),
	}
	lse.lsc.uncommittedLLSNEnd.Store(uncommittedLLSNEnd)

	bw := &backupWriter{}
	bw.queue = make(chan *backupWriteTask, 1)
	bw.logger = zap.NewNop()
	bw.lse = lse

	assert.Panics(t, func() {
		wb := stg.NewWriteBatch()
		err := wb.Set(uncommittedLLSNEnd+1, nil)
		assert.NoError(t, err)
		bwt := newBackupWriteTask(wb, uncommittedLLSNEnd+1, uncommittedLLSNEnd+2)
		bw.writeLoopInternal(context.Background(), bwt)
	})
}
