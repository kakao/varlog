package executor

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/util/jobqueue"
)

// commitTaskQueue is type-safe wrapper of jobqueue.Queue.
type commitTaskQueue interface {
	pushWithContext(ctx context.Context, ctb *commitTask) error
	popWithContext(ctx context.Context) (*commitTask, error)
	pop() *commitTask
	size() int
}

type commitTaskQueueImpl struct {
	q        jobqueue.JobQueue
	capacity int
}

var _ commitTaskQueue = (*commitTaskQueueImpl)(nil)

func newCommitTaskQueue(queueSize int) (*commitTaskQueueImpl, error) {
	ctq := &commitTaskQueueImpl{capacity: queueSize}
	q, err := jobqueue.NewChQueue(queueSize)
	if err != nil {
		return nil, err
	}
	ctq.q = q
	return ctq, nil
}

func (ctq *commitTaskQueueImpl) pushWithContext(ctx context.Context, ctb *commitTask) error {
	return ctq.q.PushWithContext(ctx, ctb)
}

func (ctq *commitTaskQueueImpl) popWithContext(ctx context.Context) (*commitTask, error) {
	item, err := ctq.q.PopWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return item.(*commitTask), nil
}

func (ctq *commitTaskQueueImpl) pop() *commitTask {
	return ctq.q.Pop().(*commitTask)
}

func (ctq *commitTaskQueueImpl) size() int {
	return ctq.q.Size()
}
