package logstream

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/pkg/types"
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
	err := bw.send(context.Background(), &ReplicationTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateSealed)
	err = bw.send(context.Background(), &ReplicationTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateClosed)
	err = bw.send(context.Background(), &ReplicationTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateAppendable)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = bw.send(canceledCtx, &ReplicationTask{})
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
	bw.queue = make(chan *ReplicationTask, numTasks)
	bw.logger = zap.NewNop()

	for i := 0; i < numTasks; i++ {
		rst := NewReplicationTask()
		rst.Req.BeginLLSN = types.LLSN(i + 1)
		rst.Req.Data = [][]byte{nil}
		err := bw.send(context.Background(), rst)
		assert.NoError(t, err)
	}

	assert.EqualValues(t, numTasks, bw.inflight.Load())
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

	assert.Zero(t, bw.inflight.Load())
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
	lse.stg = stg

	bw := &backupWriter{}
	bw.queue = make(chan *ReplicationTask, 1)
	bw.logger = zap.NewNop()
	bw.lse = lse

	rst := NewReplicationTask()
	rst.Req.BeginLLSN = types.LLSN(uncommittedLLSNEnd + 1)
	rst.Req.Data = [][]byte{nil}
	bw.writeLoopInternal(context.Background(), []*ReplicationTask{rst})

	// Keep the uncommittedLLSNEnd unchanged.
	require.Equal(t, uncommittedLLSNEnd, bw.lse.lsc.uncommittedLLSNEnd.Load())
}
