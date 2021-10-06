package executor

import (
	"context"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/util/jobqueue"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

type replicateQueue interface {
	pushWithContext(ctx context.Context, t *replicateTask) error
	popWithContext(ctx context.Context) (*replicateTask, error)
	pop() *replicateTask
	size() int
}

type replicateQueueImpl struct {
	q        jobqueue.JobQueue
	capacity int
}

var _ replicateQueue = (*replicateQueueImpl)(nil)

func newReplicateQueue(queueSize int) (*replicateQueueImpl, error) {
	cq := &replicateQueueImpl{capacity: queueSize}
	q, err := jobqueue.NewChQueue(queueSize)
	if err != nil {
		return nil, err
	}
	cq.q = q
	return cq, nil
}

func (rq *replicateQueueImpl) pushWithContext(ctx context.Context, t *replicateTask) error {
	if t == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	return rq.q.PushWithContext(ctx, t)
}

func (rq *replicateQueueImpl) popWithContext(ctx context.Context) (*replicateTask, error) {
	item, err := rq.q.PopWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return item.(*replicateTask), nil
}

func (rq *replicateQueueImpl) pop() *replicateTask {
	return rq.q.Pop().(*replicateTask)
}

func (rq *replicateQueueImpl) size() int {
	return rq.q.Size()
}
