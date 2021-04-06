package stopchannel

import (
	"sync"
)

type StopChannel struct {
	c      chan struct{}
	closed bool
	lock   sync.RWMutex
	once   sync.Once
}

func New() *StopChannel {
	return &StopChannel{
		c: make(chan struct{}),
	}
}

func (sc *StopChannel) Stop() {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	sc.once.Do(func() {
		close(sc.c)
		sc.closed = true
	})
}

func (sc *StopChannel) Stopped() bool {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	return sc.closed
}

func (sc *StopChannel) StopC() <-chan struct{} {
	return sc.c
}
