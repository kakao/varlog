package logstream

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestCommitWaitQueue(t *testing.T) {
	const n = 10

	cwq := newCommitWaitQueue()
	assert.Zero(t, cwq.size())

	assert.Nil(t, cwq.pop())

	iter := cwq.peekIterator()
	assert.False(t, iter.valid())
	assert.False(t, iter.next())
	assert.Nil(t, iter.task())

	assert.Panics(t, func() { _ = cwq.push(nil) })

	for i := 0; i < n; i++ {
		assert.Equal(t, i, cwq.size())
		cwt := newCommitWaitTask(&appendWaitGroup{
			beginLSN: varlogpb.LogSequenceNumber{
				LLSN: types.LLSN(i + 1),
			},
			batchLen: 1,
		}, 1)
		err := cwq.push(cwt)
		assert.NoError(t, err)
		assert.Equal(t, i+1, cwq.size())
	}

	iter = cwq.peekIterator()
	for i := 0; i < n; i++ {
		assert.True(t, iter.valid())
		cwt := iter.task()
		assert.Equal(t, 1, cwt.awg.batchLen)
		assert.Equal(t, types.LLSN(i+1), cwt.awg.beginLSN.LLSN)
		valid := iter.next()
		assert.Equal(t, i < n-1, valid)
	}

	for i := 0; i < n; i++ {
		cwt := cwq.pop()
		assert.Equal(t, 1, cwt.awg.batchLen)
		assert.Equal(t, types.LLSN(i+1), cwt.awg.beginLSN.LLSN)
		cwt.release()
	}
	assert.Nil(t, cwq.pop())
}

func TestCommitWaitQueueConcurrentPushPop(t *testing.T) {
	const (
		numRepeat  = 100
		cwtsLength = 128
	)

	cwq := newCommitWaitQueue()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < numRepeat; i++ {
			for j := 0; j < cwtsLength; j++ {
				awg := newAppendWaitGroup(nil, 1)
				awg.setBeginGLSN(types.GLSN(cwtsLength*i + j))
				cwt := newCommitWaitTask(awg, 1)
				err := cwq.push(cwt)
				assert.NoError(t, err)
			}
			runtime.Gosched()
		}
	}()
	go func() {
		defer wg.Done()
		cnt := 0
		for cnt < numRepeat*cwtsLength {
			num := cwq.size()
			for i := 0; i < num; i++ {
				cwt := cwq.pop()
				assert.NotNil(t, cwt)
			}
			cnt += num
		}
	}()
	wg.Wait()
}
