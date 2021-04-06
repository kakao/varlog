package atomicutil

import (
	"math"
	"sync/atomic"
	"time"
)

type AtomicBool uint32

func (b *AtomicBool) Load() bool {
	return atomic.LoadUint32((*uint32)(b)) == 1
}

func (b *AtomicBool) Store(val bool) {
	var v uint32 = 0
	if val {
		v = 1
	}
	atomic.StoreUint32((*uint32)(b), v)
}

func (b *AtomicBool) CompareAndSwap(old, new bool) (swapped bool) {
	var ov uint32 = 0
	if old {
		ov = 1
	}
	var nv uint32 = 0
	if new {
		nv = 1
	}
	swapped = atomic.CompareAndSwapUint32((*uint32)(b), ov, nv)
	return swapped
}

type AtomicTime struct {
	atomic.Value
}

func (t *AtomicTime) Load() time.Time {
	ret := t.Value.Load()
	if ret == nil {
		return time.Time{}
	}
	return ret.(time.Time)
}

func (t *AtomicTime) Store(tm time.Time) {
	t.Value.Store(tm)
}

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

type AtomicFloat64 uint64

func (b *AtomicFloat64) Load() float64 {
	return math.Float64frombits(atomic.LoadUint64((*uint64)(b)))
}

func (b *AtomicFloat64) Store(val float64) {
	atomic.StoreUint64((*uint64)(b), math.Float64bits(val))
}
