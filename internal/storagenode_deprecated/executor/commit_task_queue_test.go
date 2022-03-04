package executor

/*
import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/pkg/types"
)

func TestCommitTaskQueueZeroSize(t *testing.T) {
	_, err := newCommitTaskQueue(0)
	require.Error(t, err)
}

func TestCommitTaskQueuePushPop(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		queueSize = 10
	)

	wq, err := newCommitTaskQueue(queueSize)
	require.NoError(t, err)

	err = wq.push(context.TODO(), nil)
	require.Error(t, err)

	for i := 0; i < queueSize; i++ {
		ctb := &commitTaskBlock{highWatermark: types.GLSN(i)}
		wq.push(context.TODO(), ctb)
	}
	require.Equal(t, queueSize, wq.queue.q.Size())

	for i := 0; i < queueSize; i++ {
		ctb, err := wq.pop(context.TODO())
		require.NoError(t, err)
		require.Equal(t, types.GLSN(i), ctb.highWatermark)
	}
	require.Zero(t, wq.queue.q.Size())
}

func TestCommitTaskQueueDrain(t *testing.T) {
	defer goleak.VerifyNone(t)

	const queueSize = 10

	wq, err := newCommitTaskQueue(queueSize)
	require.NoError(t, err)

	for i := 0; i < queueSize; i++ {
		err := wq.push(context.TODO(), &commitTaskBlock{})
		require.NoError(t, err)
	}

	numDropped := 0
	wq.drain(func(*commitTaskBlock) {
		numDropped++
	})
	require.Equal(t, queueSize, numDropped)
}

func TestCommitTaskQueueContextError(t *testing.T) {
	defer goleak.VerifyNone(t)

	const queueSize = 10

	wq, err := newCommitTaskQueue(queueSize)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.TODO())
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		for {
			if err := wq.push(ctx, &commitTaskBlock{}); err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			if _, err := wq.pop(ctx); err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			if _, err := wq.wait(ctx); err != nil {
				return
			}
		}
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()
}

func TestCommitTaskQueueClose(t *testing.T) {
	defer goleak.VerifyNone(t)

	const queueSize = 10

	wq, err := newCommitTaskQueue(queueSize)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		for {
			if err := wq.push(context.TODO(), &commitTaskBlock{}); err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			if _, err := wq.pop(context.TODO()); err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			if _, err := wq.wait(context.TODO()); err != nil {
				return
			}
		}
	}()
	time.Sleep(50 * time.Millisecond)
	wq.close()
	wg.Wait()
}
*/
