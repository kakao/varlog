package stopwaiter

import (
	"sync"
)

type StopWaiter struct {
	mu      sync.RWMutex
	stopped bool
	stopc   chan struct{}
	onStop  func()
}

func New() *StopWaiter {
	return NewWithOnStop(nil)
}

func NewWithOnStop(onStop func()) *StopWaiter {
	return &StopWaiter{stopc: make(chan struct{}), onStop: onStop}
}

func (s *StopWaiter) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	close(s.stopc)
	s.stopped = true
	if s.onStop != nil {
		go s.onStop()
	}
}

func (s *StopWaiter) Stopped() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stopped
}

func (s *StopWaiter) Wait() {
	<-s.stopc
}
