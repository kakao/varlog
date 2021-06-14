package executor

import (
	"context"

	"github.com/kakao/varlog/internal/storagenode/jobqueue"
)

// writeQueue is type-safe wrapper of jobqueue.Queue.
type writeQueue interface {
	pushWithContext(ctx context.Context, task *writeTask) error
	popWithContext(ctx context.Context) (*writeTask, error)
	pop() *writeTask
	size() int
}

type writeQueueImpl struct {
	q        jobqueue.JobQueue
	capacity int
}

var _ writeQueue = (*writeQueueImpl)(nil)

func newWriteQueue(queueSize int) (*writeQueueImpl, error) {
	wq := &writeQueueImpl{
		capacity: queueSize,
	}
	q, err := jobqueue.NewChQueue(queueSize)
	if err != nil {
		return nil, err
	}
	wq.q = q
	return wq, nil
}

func (wq *writeQueueImpl) pushWithContext(ctx context.Context, task *writeTask) error {
	return wq.q.PushWithContext(ctx, task)
}

func (wq *writeQueueImpl) popWithContext(ctx context.Context) (*writeTask, error) {
	item, err := wq.q.PopWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return item.(*writeTask), nil
}

func (wq *writeQueueImpl) pop() *writeTask {
	return wq.q.Pop().(*writeTask)
}

func (wq *writeQueueImpl) size() int {
	return wq.q.Size()
}
