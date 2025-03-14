package logstream

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestCommitter_InvalidConfig(t *testing.T) {
	_, err := newCommitter(committerConfig{
		commitQueueCapacity: minQueueCapacity - 1,
		lse:                 &Executor{},
		logger:              zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newCommitter(committerConfig{
		commitQueueCapacity: maxQueueCapacity + 1,
		lse:                 &Executor{},
		logger:              zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newCommitter(committerConfig{
		commitQueueCapacity: 0,
		lse:                 nil,
		logger:              zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newCommitter(committerConfig{
		commitQueueCapacity: 0,
		lse:                 &Executor{},
		logger:              nil,
	})
	assert.Error(t, err)
}

func TestCommitter_ShouldNotAcceptTasksWhileNotAppendable(t *testing.T) {
	lse := &Executor{
		esm: newExecutorStateManager(executorStateSealing),
	}
	cm := &committer{}
	cm.lse = lse
	cm.logger = zap.NewNop()

	lse.esm.store(executorStateAppendable)
	assert.Panics(t, func() {
		_ = cm.sendCommitWaitTask(context.Background(), &commitWaitTask{}, false /*ignoreSealing*/)
	})

	assert.Panics(t, func() {
		_ = cm.sendCommitWaitTask(context.Background(), nil, false /*ignoreSealing*/)
	})

	cwt := &commitWaitTask{
		awg: &appendWaitGroup{
			beginLSN: varlogpb.LogSequenceNumber{
				LLSN: types.MinLLSN,
			},
		},
		size: 1,
	}

	lse.esm.store(executorStateSealing)
	err := cm.sendCommitWaitTask(context.Background(), cwt, false /*ignoreSealing*/)
	assert.Error(t, err)

	lse.esm.store(executorStateSealed)
	err = cm.sendCommitWaitTask(context.Background(), cwt, false /*ignoreSealing*/)
	assert.Error(t, err)

	lse.esm.store(executorStateClosed)
	err = cm.sendCommitWaitTask(context.Background(), cwt, false /*ignoreSealing*/)
	assert.Error(t, err)

	// sendCommitTask
	lse.esm.store(executorStateAppendable)
	assert.Panics(t, func() {
		_ = cm.sendCommitTask(context.Background(), nil)
	})

	lse.esm.store(executorStateSealed)
	err = cm.sendCommitTask(context.Background(), &commitTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateClosed)
	err = cm.sendCommitTask(context.Background(), &commitTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateAppendable)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = cm.sendCommitTask(canceledCtx, &commitTask{})
	assert.Error(t, err)
}

func TestCommitter_DrainCommitQueue(t *testing.T) {
	const numCommitTasks = 10

	lse := &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}
	cm := &committer{
		committerConfig: committerConfig{
			commitQueueCapacity: numCommitTasks,
			lse:                 lse,
			logger:              zap.NewNop(),
		},
		commitQueue: make(chan *commitTask, numCommitTasks),
	}

	for i := 0; i < numCommitTasks; i++ {
		ct := newCommitTask()
		err := cm.sendCommitTask(context.Background(), ct)
		assert.NoError(t, err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cm.waitForDrainageOfCommitQueue(false)
	}()

	runtime.Gosched()
	cm.waitForDrainageOfCommitQueue(true)

	wg.Wait()
}

func TestCommitter_DrainCommitWaitQ(t *testing.T) {
	lse := &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}

	cm := &committer{}
	cm.commitWaitQ = newCommitWaitQueue()
	cm.lse = lse
	cm.logger = zap.NewNop()

	awg := newAppendWaitGroup(newWriteWaitGroup(), 1)
	cwt := newCommitWaitTask(awg, 1)
	err := cm.sendCommitWaitTask(context.Background(), cwt, false /*ignoreSealing*/)
	assert.NoError(t, err)

	assert.EqualValues(t, 1, cm.inflightCommitWait.Load())
	assert.EqualValues(t, 1, cm.commitWaitQ.size())

	cm.drainCommitWaitQ(errors.New("drain"))
	assert.Zero(t, cm.inflightCommitWait.Load())
	assert.Zero(t, cm.commitWaitQ.size())
}
