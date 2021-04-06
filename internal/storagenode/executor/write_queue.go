package executor

import (
	"context"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/jobqueue"
)

// writeQueue is type-safe wrapper of jobqueue.Queue.
type writeQueue interface {
	pushWithContext(ctx context.Context, task *appendTask) error
	popWithContext(ctx context.Context) (*appendTask, error)
	pop() *appendTask
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

func (wq *writeQueueImpl) pushWithContext(ctx context.Context, task *appendTask) error {
	return wq.q.PushWithContext(ctx, task)
}

func (wq *writeQueueImpl) popWithContext(ctx context.Context) (*appendTask, error) {
	item, err := wq.q.PopWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return item.(*appendTask), nil
}

func (wq *writeQueueImpl) pop() *appendTask {
	return wq.q.Pop().(*appendTask)
}

func (wq *writeQueueImpl) size() int {
	return wq.q.Size()
}
