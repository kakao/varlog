package storage

import (
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/kakao/varlog/pkg/types"
)

var commitBatchPool = sync.Pool{
	New: func() interface{} {
		return &CommitBatch{
			cc: make([]byte, commitContextLength),
			ck: make([]byte, commitKeyLength),
			dk: make([]byte, dataKeyLength),
		}
	},
}

type CommitBatch struct {
	batch     *pebble.Batch
	writeOpts *pebble.WriteOptions
	cc        []byte
	ck        []byte
	dk        []byte
}

func newCommitBatch(batch *pebble.Batch, writeOpts *pebble.WriteOptions) *CommitBatch {
	cb := commitBatchPool.Get().(*CommitBatch)
	cb.batch = batch
	cb.writeOpts = writeOpts
	return cb
}

func (cb *CommitBatch) release() {
	cb.batch = nil
	cb.writeOpts = nil
	commitBatchPool.Put(cb)
}

func (cb *CommitBatch) Set(llsn types.LLSN, glsn types.GLSN) error {
	return cb.batch.Set(encodeCommitKeyInternal(glsn, cb.ck), encodeDataKeyInternal(llsn, cb.dk), nil)
}

func (cb *CommitBatch) Apply() error {
	return cb.batch.Commit(cb.writeOpts)
}

func (cb *CommitBatch) Close() error {
	err := cb.batch.Close()
	cb.release()
	return err
}
