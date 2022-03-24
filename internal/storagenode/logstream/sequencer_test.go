package logstream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storage"
)

func TestSequencer_InvalidConfig(t *testing.T) {
	defer goleak.VerifyNone(t)

	_, err := newSequencer(sequencerConfig{
		queueCapacity: minQueueCapacity - 1,
		lse:           &Executor{},
		logger:        zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newSequencer(sequencerConfig{
		queueCapacity: maxQueueCapacity + 1,
		lse:           &Executor{},
		logger:        zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newSequencer(sequencerConfig{
		logger: zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newSequencer(sequencerConfig{
		lse: &Executor{},
	})
	assert.Error(t, err)
}

func TestSequencer_ShouldNotAcceptTasksWhileNotAppendable(t *testing.T) {
	defer goleak.VerifyNone(t)

	lse := &Executor{
		esm: newExecutorStateManager(executorStateSealing),
	}
	sq := sequencer{}
	sq.logger = zap.NewNop()
	sq.lse = lse

	lse.esm.store(executorStateSealing)
	err := sq.send(context.Background(), &sequenceTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateSealed)
	err = sq.send(context.Background(), &sequenceTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateClosed)
	err = sq.send(context.Background(), &sequenceTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateAppendable)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = sq.send(canceledCtx, &sequenceTask{})
	assert.Error(t, err)
}

func testSequenceTask(stg *storage.Storage) *sequenceTask {
	st := newSequenceTask()

	st.wwg = newWriteWaitGroup()
	awg := newAppendWaitGroup(st.wwg)
	st.awgs = append(st.awgs, awg)

	st.wb = stg.NewWriteBatch() // .Deferred(0)
	st.dataBatch = [][]byte{nil}
	// st.dwb.PutData(nil)

	st.cwts = newListQueue()
	st.cwts.PushFront(newCommitWaitTask(awg))

	return st
}

func TestSequencer_FailToSendToCommitter(t *testing.T) {
	defer goleak.VerifyNone(t)

	stg := storage.TestNewStorage(t)
	defer func() {
		assert.NoError(t, stg.Close())
	}()

	cm := &committer{}
	cm.logger = zap.NewNop()
	cm.commitWaitQ = newCommitWaitQueue()
	cm.lse = &Executor{
		esm: newExecutorStateManager(executorStateSealing),
	}

	wr := &writer{}
	wr.logger = zap.NewNop()
	wr.lse = &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}
	wr.queue = make(chan *sequenceTask, 1)

	sq := &sequencer{}
	sq.logger = zap.NewNop()
	sq.lse = &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}
	sq.lse.wr = wr
	sq.lse.cm = cm

	st := testSequenceTask(stg)
	sq.sequenceLoopInternal(context.Background(), st)
	assert.Len(t, wr.queue, 0)
	assert.Equal(t, 0, cm.commitWaitQ.size())
	assert.Equal(t, executorStateSealing, sq.lse.esm.load())
}

func TestSequencer_FailToSendToWriter(t *testing.T) {
	defer goleak.VerifyNone(t)

	stg := storage.TestNewStorage(t)
	defer func() {
		assert.NoError(t, stg.Close())
	}()

	cm := &committer{}
	cm.logger = zap.NewNop()
	cm.commitWaitQ = newCommitWaitQueue()
	cm.lse = &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}

	wr := &writer{}
	wr.logger = zap.NewNop()
	wr.lse = &Executor{
		esm: newExecutorStateManager(executorStateSealing),
	}
	wr.queue = make(chan *sequenceTask, 1)

	sq := &sequencer{}
	sq.logger = zap.NewNop()
	sq.lse = &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}
	sq.lse.cm = cm
	sq.lse.wr = wr

	st := testSequenceTask(stg)
	sq.sequenceLoopInternal(context.Background(), st)
	assert.Empty(t, wr.queue)
	assert.Equal(t, executorStateSealing, sq.lse.esm.load())
}

func TestSequencer_FailToSendToReplicateClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	stg := storage.TestNewStorage(t)
	defer func() {
		assert.NoError(t, stg.Close())
	}()

	wr := &writer{}
	wr.logger = zap.NewNop()
	wr.lse = &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}
	wr.queue = make(chan *sequenceTask, 1)

	cm := &committer{}
	cm.logger = zap.NewNop()
	cm.commitWaitQ = newCommitWaitQueue()
	cm.lse = &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}

	rc := &replicateClient{}
	rc.logger = zap.NewNop()
	rc.queue = make(chan *replicateTask, 1)
	rc.lse = &Executor{
		esm: newExecutorStateManager(executorStateSealing),
	}

	sq := &sequencer{}
	sq.logger = zap.NewNop()
	sq.lse = &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}
	sq.lse.wr = wr
	sq.lse.cm = cm
	sq.lse.rcs = &replicateClients{
		clients: []*replicateClient{rc},
	}

	st := testSequenceTask(stg)
	st.rts = []*replicateTask{
		{},
	}
	sq.sequenceLoopInternal(context.Background(), st)
	assert.Len(t, wr.queue, 1)
	assert.Equal(t, 1, cm.commitWaitQ.size())
	assert.Empty(t, rc.queue)
	assert.Equal(t, executorStateSealing, sq.lse.esm.load())
}

func TestSequencer_Drain(t *testing.T) {
	defer goleak.VerifyNone(t)

	const numTasks = 10

	stg := storage.TestNewStorage(t)
	defer func() {
		assert.NoError(t, stg.Close())
	}()

	lse := &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
		lsc: newLogStreamContext(),
	}

	sq := &sequencer{}
	sq.lse = lse
	sq.logger = zap.NewNop()
	sq.queue = make(chan *sequenceTask, numTasks)

	wr := &writer{}
	wr.lse = lse

	cm := &committer{}
	cm.lse = lse

	lse.sq = sq
	lse.wr = wr
	lse.cm = cm

	for i := 0; i < numTasks; i++ {
		err := sq.send(context.Background(), testSequenceTask(stg))
		assert.NoError(t, err)
	}

	assert.EqualValues(t, numTasks, atomic.LoadInt64(&sq.inflight))
	assert.Len(t, sq.queue, numTasks)

	lse.esm.store(executorStateSealing)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sq.sequenceLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		sq.waitForDrainage(nil, false)
	}()

	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&sq.inflight) == 0 && len(sq.queue) == 0
	}, time.Second, 10*time.Millisecond)

	cancel()
	wg.Wait()
}

func TestSequencer_ForceDrain(t *testing.T) {
	defer goleak.VerifyNone(t)

	const numTasks = 10

	stg := storage.TestNewStorage(t)
	defer func() {
		assert.NoError(t, stg.Close())
	}()

	lse := &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}

	sq := &sequencer{}
	sq.queue = make(chan *sequenceTask, numTasks)
	sq.lse = lse
	sq.logger = zap.NewNop()

	lse.sq = sq

	for i := 0; i < numTasks; i++ {
		err := sq.send(context.Background(), testSequenceTask(stg))
		assert.NoError(t, err)
	}

	assert.EqualValues(t, numTasks, atomic.LoadInt64(&sq.inflight))
	assert.Len(t, sq.queue, numTasks)

	sq.waitForDrainage(errors.New("force drain"), true)

	assert.Zero(t, atomic.LoadInt64(&sq.inflight))
	assert.Empty(t, sq.queue)
}
