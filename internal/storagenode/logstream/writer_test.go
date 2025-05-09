package logstream

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/pkg/types"
)

func TestWriter_InvalidConfig(t *testing.T) {
	_, err := newWriter(writerConfig{
		queueCapacity: minQueueCapacity - 1,
		lse:           &Executor{},
		logger:        zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newWriter(writerConfig{
		queueCapacity: maxQueueCapacity + 1,
		lse:           &Executor{},
		logger:        zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newWriter(writerConfig{
		queueCapacity: 0,
		lse:           nil,
		logger:        zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newWriter(writerConfig{
		queueCapacity: 0,
		lse:           &Executor{},
		logger:        nil,
	})
	assert.Error(t, err)
}

func TestWriter_ShouldNotAcceptTasksWhileNotAppendable(t *testing.T) {
	lse := &Executor{
		esm: newExecutorStateManager(executorStateSealing),
	}
	wr := &writer{}
	wr.lse = lse
	wr.logger = zap.NewNop()

	lse.esm.store(executorStateSealing)
	err := wr.send(context.Background(), &sequenceTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateSealed)
	err = wr.send(context.Background(), &sequenceTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateClosed)
	err = wr.send(context.Background(), &sequenceTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateAppendable)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = wr.send(canceledCtx, &sequenceTask{})
	assert.Error(t, err)
}

func TestWriter_DrainForce(t *testing.T) {
	const numTasks = 10

	stg := storage.TestNewStorage(t)
	defer func() {
		assert.NoError(t, stg.Close())
	}()

	lse := &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}
	lse.stg = stg

	wr := &writer{}
	wr.lse = lse
	wr.logger = zap.NewNop()
	wr.queue = make(chan *sequenceTask, numTasks)

	for i := 0; i < numTasks; i++ {
		st := testSequenceTask()
		err := wr.send(context.Background(), st)
		assert.NoError(t, err)
	}

	assert.EqualValues(t, numTasks, wr.inflight.Load())
	assert.Len(t, wr.queue, numTasks)
	wr.waitForDrainage(errors.New("force drain"), true)
	assert.Zero(t, wr.inflight.Load())
	assert.Empty(t, wr.queue)
}

func TestWriter_UnexpectedLLSN(t *testing.T) {
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

	wr := &writer{}
	wr.queue = make(chan *sequenceTask, 1)
	wr.logger = zap.NewNop()
	wr.lse = lse

	st := testSequenceTask()
	st.awg.setBeginLLSN(uncommittedLLSNEnd - 1) // not expected LLSN
	wr.writeLoopInternal(context.Background(), []*sequenceTask{st})

	// Keep the uncommittedLLSNEnd unchanged.
	require.Equal(t, uncommittedLLSNEnd, lse.lsc.uncommittedLLSNEnd.Load())
}
