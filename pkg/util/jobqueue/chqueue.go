package jobqueue

import (
	"context"

	"github.com/kakao/varlog/pkg/verrors"
)

type chQueue struct {
	queue    chan interface{}
	capacity int
}

var _ JobQueue = (*chQueue)(nil)

func NewChQueue(queueSize int) (JobQueue, error) {
	if queueSize <= 0 {
		return nil, verrors.ErrInvalid
	}
	q := &chQueue{
		queue:    make(chan interface{}, queueSize),
		capacity: queueSize,
	}
	return q, nil
}

func (q *chQueue) PushWithContext(ctx context.Context, item interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.queue <- item:
		return nil
	}
}

func (q *chQueue) PopWithContext(ctx context.Context) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case item := <-q.queue:
		return item, nil
	}
}

func (q *chQueue) Pop() interface{} {
	return <-q.queue
}

func (q *chQueue) Size() int {
	return len(q.queue)
}
