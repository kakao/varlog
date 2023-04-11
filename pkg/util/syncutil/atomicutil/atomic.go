package atomicutil

import (
	"sync/atomic"
	"time"
)

type AtomicDuration time.Duration

func (d *AtomicDuration) Load() time.Duration {
	return time.Duration(atomic.LoadInt64((*int64)(d)))
}

func (d *AtomicDuration) Store(duration time.Duration) {
	atomic.StoreInt64((*int64)(d), int64(duration))
}

func (d *AtomicDuration) Add(duration time.Duration) {
	atomic.AddInt64((*int64)(d), int64(duration))
}
