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
