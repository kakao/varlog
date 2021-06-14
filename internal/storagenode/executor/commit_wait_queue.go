package executor

import (
	"container/list"
	"sync"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

type commitWaitQueueIterator interface {
	task() *commitWaitTask
	next() bool
	valid() bool
}

type commitWaitQueueIteratorImpl struct {
	curr  *list.Element
	queue *commitWaitQueueImpl
}

var _ commitWaitQueueIterator = (*commitWaitQueueIteratorImpl)(nil)

func (iter *commitWaitQueueIteratorImpl) task() *commitWaitTask {
	return iter.curr.Value.(*commitWaitTask)
}

func (iter *commitWaitQueueIteratorImpl) next() bool {
	iter.queue.mu.RLock()
	defer iter.queue.mu.RUnlock()
	iter.curr = iter.curr.Prev()
	return iter.valid()
}

func (iter *commitWaitQueueIteratorImpl) valid() bool {
	return iter.curr != nil
}

type commitWaitQueue interface {
	push(tb *commitWaitTask) error
	peekIterator() commitWaitQueueIterator
	pop() *commitWaitTask
	size() int
}

type commitWaitQueueImpl struct {
	queue *list.List
	mu    sync.RWMutex
}

var _ commitWaitQueue = (*commitWaitQueueImpl)(nil)

func newCommitWaitQueue() (commitWaitQueue, error) {
	cwq := &commitWaitQueueImpl{
		queue: list.New(),
	}
	return cwq, nil
}

func (cwq *commitWaitQueueImpl) push(cwt *commitWaitTask) error {
	if cwt == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	cwq.mu.Lock()
	cwq.queue.PushFront(cwt)
	cwq.mu.Unlock()
	return nil
}

func (cwq *commitWaitQueueImpl) peekIterator() commitWaitQueueIterator {
	cwq.mu.RLock()
	iter := &commitWaitQueueIteratorImpl{
		curr:  cwq.queue.Back(),
		queue: cwq,
	}
	cwq.mu.RUnlock()
	return iter
}

func (cwq *commitWaitQueueImpl) pop() *commitWaitTask {
	cwq.mu.Lock()
	defer cwq.mu.Unlock()
	elem := cwq.queue.Back()
	if elem == nil {
		return nil
	}
	return cwq.queue.Remove(elem).(*commitWaitTask)
}

func (cwq *commitWaitQueueImpl) size() int {
	cwq.mu.RLock()
	ret := cwq.queue.Len()
	cwq.mu.RUnlock()
	return ret
}
