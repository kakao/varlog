package executor

import (
	"container/list"
	"sync"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/verrors"
)

type commitWaitQueueIterator interface {
	task() *appendTask
	next() bool
	valid() bool
}

type commitWaitQueueIteratorImpl struct {
	curr  *list.Element
	queue *commitWaitQueueImpl
}

var _ commitWaitQueueIterator = (*commitWaitQueueIteratorImpl)(nil)

func (iter *commitWaitQueueIteratorImpl) task() *appendTask {
	return iter.curr.Value.(*appendTask)
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
	push(tb *appendTask) error
	peekIterator() commitWaitQueueIterator
	pop() *appendTask
	size() int
}

type commitWaitQueueImpl struct {
	queue *list.List
	mu    sync.RWMutex
}

var _ commitWaitQueue = (*commitWaitQueueImpl)(nil)

func newCommitWaitQueue() (commitWaitQueue, error) {
	cq := &commitWaitQueueImpl{
		queue: list.New(),
	}
	return cq, nil
}

func (cq *commitWaitQueueImpl) push(tb *appendTask) error {
	if tb == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	cq.mu.Lock()
	cq.queue.PushFront(tb)
	cq.mu.Unlock()
	return nil
}

func (cq *commitWaitQueueImpl) peekIterator() commitWaitQueueIterator {
	cq.mu.RLock()
	iter := &commitWaitQueueIteratorImpl{
		curr:  cq.queue.Back(),
		queue: cq,
	}
	cq.mu.RUnlock()
	return iter
}

func (cq *commitWaitQueueImpl) pop() *appendTask {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	elem := cq.queue.Back()
	if elem == nil {
		return nil
	}
	return cq.queue.Remove(elem).(*appendTask)
}

func (cq *commitWaitQueueImpl) size() int {
	cq.mu.RLock()
	ret := cq.queue.Len()
	cq.mu.RUnlock()
	return ret
}
