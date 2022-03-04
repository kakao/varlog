package logstream

import "sync"

var nodePool = sync.Pool{
	New: func() interface{} {
		return &node{}
	},
}

type node struct {
	prev  *node
	next  *node
	value interface{}
}

func newNode(value interface{}) *node {
	n := nodePool.Get().(*node)
	n.value = value
	return n
}

func (n *node) release() {
	n.prev = nil
	n.next = nil
	n.value = nil
	nodePool.Put(n)
}

func (n *node) Prev() *node {
	if n == nil || n.prev.prev == nil {
		return nil
	}
	return n.prev
}

var listQueuePool = sync.Pool{
	New: func() interface{} {
		lq := &listQueue{}
		lq.reset()
		return lq
	},
}

type listQueue struct {
	front node
	back  node
	len   int
}

func newListQueue() *listQueue {
	return listQueuePool.Get().(*listQueue)
}

func (lq *listQueue) release() {
	lq.reset()
	listQueuePool.Put(lq)
}

func (lq *listQueue) reset() {
	lq.front.prev = nil
	lq.front.next = &lq.back
	lq.back.prev = &lq.front
	lq.back.next = nil
	lq.len = 0
}

func (lq *listQueue) PushFront(value interface{}) *node {
	n := newNode(value)
	if lq.len == 0 {
		lq.front.next = n
		lq.back.prev = n
		n.next = &lq.back
		n.prev = &lq.front
		lq.len++
		return n
	}
	n.prev = &lq.front
	n.next = lq.front.next
	lq.front.next.prev = n
	lq.front.next = n
	lq.len++
	return n
}

func (lq *listQueue) Back() *node {
	if lq.len == 0 {
		return nil
	}
	return lq.back.prev
}

func (lq *listQueue) RemoveBack() interface{} {
	if lq.len == 0 {
		return nil
	}
	n := lq.back.prev
	n.prev.next = &lq.back
	lq.back.prev = n.prev
	lq.len--

	value := n.value
	n.release()
	return value
}

func (lq *listQueue) Len() int {
	return lq.len
}

func (lq *listQueue) ConcatFront(other *listQueue) {
	if lq.len == 0 {
		lq.front.next = other.front.next
		lq.front.next.prev = &lq.front
		lq.back.prev = other.back.prev
		lq.back.prev.next = &lq.back
		lq.len = other.len
		other.reset()
		return
	}
	if other.len == 0 {
		other.reset()
		return
	}
	lq.front.next.prev = other.back.prev
	other.back.prev.next = lq.front.next
	lq.front.next = other.front.next
	lq.front.next.prev = &lq.front
	lq.len += other.len
	other.reset()
}
