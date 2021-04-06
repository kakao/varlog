package executor

/*
import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type commitQueueMock struct {
	mock.Mock
}

func (cq *commitQueueMock) push(ctx context.Context, taskBlocks ...*taskBlock) error {
	args := cq.Called(ctx, taskBlocks)
	return args.Error(0)
}

func (cq *commitQueueMock) pop(ctx context.Context) (*taskBlock, error) {
	args := cq.Called(ctx)
	return args.Get(0).(*taskBlock), args.Error(1)

}

func (cq *commitQueueMock) size() int {
	return cq.Called().Int(0)
}

func (cq *commitQueueMock) close() {
}

func (cq *commitQueueMock) drain(drainFunc func(*taskBlock)) {
}

func TestCommitQueueZeroQueueSize(t *testing.T) {
	_, err := newCommitQueue(0, 0)
	require.Error(t, err)
}

func TestCommitQueueEnqueueNonSquential(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		queueSize = 10
		lastLLSN  = types.LLSN(0)
	)
	cq, err := newCommitQueue(queueSize, lastLLSN)
	require.NoError(t, err)

	tb := &taskBlock{llsn: lastLLSN + 2}
	err = cq.push(context.TODO(), tb)
	require.Error(t, err)
}

func TestCommitQueueEnqueueNil(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		queueSize = 10
		lastLLSN  = types.LLSN(0)
	)
	cq, err := newCommitQueue(queueSize, lastLLSN)
	require.NoError(t, err)

	err = cq.push(context.TODO(), nil)
	require.Error(t, err)
}

func TestCommitQueuePushPop(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		queueSize = 100
		lastLLSN  = 0
	)

	rand.Seed(time.Now().UnixNano())

	cq, err := newCommitQueue(queueSize, lastLLSN)
	require.NoError(t, err)

	for i := 1; i <= queueSize; i++ {
		tb := &taskBlock{llsn: types.LLSN(lastLLSN + i)}
		err := cq.push(context.TODO(), tb)
		require.NoError(t, err)
	}

	require.Equal(t, queueSize, cq.size())

	expectedLLSN := types.LLSN(1)
	remains := queueSize
	for remains > 0 {
		popSize := rand.Intn(remains) + 1
		remains -= popSize
		for i := 0; i < popSize; i++ {
			task := cq.pop()
			require.Equal(t, expectedLLSN, task.llsn)
			expectedLLSN++
		}
	}
}

func TestCommitQueueDrain(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		queueSize = 100
		lastLLSN  = 0
	)

	rand.Seed(time.Now().UnixNano())

	cq, err := newCommitQueue(queueSize, lastLLSN)
	require.NoError(t, err)

	for i := 1; i <= queueSize; i++ {
		tb := &taskBlock{llsn: types.LLSN(lastLLSN + i)}
		err := cq.push(context.TODO(), tb)
		require.NoError(t, err)
	}

	numDropped := 0
	cq.drain(func(*taskBlock) {
		numDropped++
	})
	require.Equal(t, queueSize, numDropped)
}

func TestCommitQueueContextError(t *testing.T) {
	t.Skip()

	defer goleak.VerifyNone(t)

	const (
		queueSize = 100
		lastLLSN  = 0
		popSize   = 5
	)

	rand.Seed(time.Now().UnixNano())

	cq, err := newCommitQueue(queueSize, lastLLSN)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.TODO())
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		i := 1
		for {
			tb := &taskBlock{llsn: types.LLSN(lastLLSN + i)}
			if err := cq.push(ctx, tb); err != nil {
				return
			}
			i++
		}
	}()
	go func() {
		defer wg.Done()
		i := 1
		for {
			tb := cq.pop()
			require.Equal(t, types.LLSN(lastLLSN+i), tb.llsn)
			i++
		}
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()
}

func TestCommitQueueClose(t *testing.T) {
	t.Skip()
		defer goleak.VerifyNone(t)

		const (
			queueSize = 100
			lastLLSN  = 0
			popSize   = 5
		)

		rand.Seed(time.Now().UnixNano())

		cq, err := newCommitQueue(queueSize, lastLLSN)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			i := 1
			for {
				tb := &taskBlock{llsn: types.LLSN(lastLLSN + i)}
				if err := cq.push(ctx, tb); err != nil {
					return
				}
				i++
			}
		}()
		go func() {
			defer wg.Done()
			i := 1
			for {
				tb, err := cq.pop(ctx)
				if err != nil {
					return
				}
				require.Equal(t, types.LLSN(lastLLSN+i), tb.llsn)
				i++
			}
		}()
		time.Sleep(50 * time.Millisecond)
		cq.close()
		wg.Wait()
}
*/
