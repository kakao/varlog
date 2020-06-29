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

type AtomicGLSN uint64

func (glsn *AtomicGLSN) Add(delta uint64) GLSN {
	return GLSN(atomic.AddUint64((*uint64)(glsn), delta))
}

func (glsn *AtomicGLSN) Load() GLSN {
	return GLSN(atomic.LoadUint64((*uint64)(glsn)))
}

func (glsn *AtomicGLSN) Store(val GLSN) {
	atomic.StoreUint64((*uint64)(glsn), uint64(val))
}

type LLSN uint64

type AtomicLLSN uint64

func (llsn *AtomicLLSN) Add(delta uint64) LLSN {
	return LLSN(atomic.AddUint64((*uint64)(llsn), delta))
}

func (llsn *AtomicLLSN) Load() LLSN {
	return LLSN(atomic.LoadUint64((*uint64)(llsn)))
}

func (llsn *AtomicLLSN) Store(val LLSN) {
	atomic.StoreUint64((*uint64)(llsn), uint64(val))
}
