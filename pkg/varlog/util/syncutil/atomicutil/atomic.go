package atomicutil

import "sync/atomic"

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
	swapped = atomic.CompareAndSwapUint32((*uint32)(b), (uint32)(ov), (uint32)(nv))
	return swapped
}
