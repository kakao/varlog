package types

import (
	"math"
	"sync/atomic"
)

type StorageNodeID int32

type LogStreamID int32

const (
	InvalidLogStreamID = LogStreamID(math.MinInt32)
)

type GLSN uint64

type AtomicGLSN struct {
	n uint64
}

func (glsn *AtomicGLSN) Add(delta uint64) GLSN {
	return GLSN(atomic.AddUint64(&glsn.n, delta))
}

func (glsn *AtomicGLSN) Load() GLSN {
	return GLSN(atomic.LoadUint64(&glsn.n))
}

func (glsn *AtomicGLSN) Store(val GLSN) {
	atomic.StoreUint64(&glsn.n, uint64(val))
}

type LLSN uint64

type AtomicLLSN struct {
	n uint64
}

func (llsn *AtomicLLSN) Add(delta uint64) LLSN {
	return LLSN(atomic.AddUint64(&llsn.n, delta))
}

func (llsn *AtomicLLSN) Load() LLSN {
	return LLSN(atomic.LoadUint64(&llsn.n))
}

func (llsn *AtomicLLSN) Store(val LLSN) {
	atomic.StoreUint64(&llsn.n, uint64(val))
}
