package storage

import "sync"

var commitKeyBufferPool = sync.Pool{
	New: func() interface{} {
		return &commitKeyBuffer{}
	},
}

type commitKeyBuffer struct {
	ck [commitKeyLength]byte
}

func newCommitKeyBuffer() *commitKeyBuffer {
	return commitKeyBufferPool.Get().(*commitKeyBuffer)
}

func (b *commitKeyBuffer) release() {
	commitKeyBufferPool.Put(b)
}

var dataKeyBufferPool = sync.Pool{
	New: func() interface{} {
		b := &dataKeyBuffer{}
		b.dk = b.buf[:]
		return b
	},
}

type dataKeyBuffer struct {
	buf [dataKeyLength]byte
	dk  []byte
}

func newDataKeyBuffer() *dataKeyBuffer {
	return dataKeyBufferPool.Get().(*dataKeyBuffer)
}

func (b *dataKeyBuffer) release() {
	dataKeyBufferPool.Put(b)
}
