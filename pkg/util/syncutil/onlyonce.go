package syncutil

import (
	"sync"
	"sync/atomic"
)

type OnlyOnce struct {
	done int32
	m    sync.Mutex
}

func (o *OnlyOnce) DoOrElse(f func() error, g func() error) error {
	if atomic.LoadInt32(&o.done) == 0 {
		return o.do(f, g)
	}
	return g()
}

func (o *OnlyOnce) Do(f func() error) error {
	return o.DoOrElse(f, func() error { return nil })
}

func (o *OnlyOnce) do(f func() error, g func() error) error {
	o.m.Lock()
	defer o.m.Unlock()
	if o.done == 0 {
		defer atomic.StoreInt32(&o.done, 1)
		return f()
	}
	return g()
}
