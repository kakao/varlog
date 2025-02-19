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
	queue         *listQueue
	numLogEntries int
	mu            sync.Mutex
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
	cwq.numLogEntries += cwt.size
	cwq.mu.Unlock()
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
	cwt := cwq.queue.RemoveBack().(*commitWaitTask)
	cwq.numLogEntries -= cwt.size
	return cwt
}

func (cwq *commitWaitQueue) size() int {
	cwq.mu.Lock()
	ret := cwq.numLogEntries
	cwq.mu.Unlock()
	return ret
}
