package logstream

import (
	"sync"
)

type commitWaitQueueIterator struct {
	curr  *node
	queue *commitWaitQueue
}

func (iter commitWaitQueueIterator) task() *commitWaitTask {
	if !iter.valid() {
		return nil
	}
	return iter.curr.value.(*commitWaitTask)
}

func (iter *commitWaitQueueIterator) next() bool {
	iter.queue.mu.Lock()
	defer iter.queue.mu.Unlock()
	if iter.curr != nil {
		iter.curr = iter.curr.Prev()
	}
	return iter.valid()
}

func (iter commitWaitQueueIterator) valid() bool {
	return iter.curr != nil
}

type commitWaitQueue struct {
	queue *listQueue
	mu    sync.Mutex
}

func newCommitWaitQueue() *commitWaitQueue {
	return &commitWaitQueue{
		queue: newListQueue(),
	}
}

func (cwq *commitWaitQueue) push(cwt *commitWaitTask) error {
	if cwt == nil {
		panic("log stream: commit wait queue: task is nil")
	}
	cwq.mu.Lock()
	cwq.queue.PushFront(cwt)
	cwq.mu.Unlock()
	return nil
}

func (cwq *commitWaitQueue) pushList(cwts *listQueue) error {
	if cwts == nil {
		panic("log stream: commit wait queue: task is nil")
	}
	if cwts.Len() == 0 {
		panic("log stream: commit wait queue: empty tasks")
	}
	cwq.mu.Lock()
	cwq.queue.ConcatFront(cwts)
	cwq.mu.Unlock()
	cwts.release()
	return nil
}

func (cwq *commitWaitQueue) peekIterator() commitWaitQueueIterator {
	cwq.mu.Lock()
	iter := commitWaitQueueIterator{
		curr:  cwq.queue.Back(),
		queue: cwq,
	}
	cwq.mu.Unlock()
	return iter
}

func (cwq *commitWaitQueue) pop() *commitWaitTask {
	cwq.mu.Lock()
	defer cwq.mu.Unlock()
	if elem := cwq.queue.Back(); elem == nil {
		return nil
	}
	return cwq.queue.RemoveBack().(*commitWaitTask)
}

func (cwq *commitWaitQueue) size() int {
	cwq.mu.Lock()
	ret := cwq.queue.Len()
	cwq.mu.Unlock()
	return ret
}
