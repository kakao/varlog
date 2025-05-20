package storage

import (
	"errors"
	"sync"

	"github.com/cockroachdb/pebble/v2"

	"github.com/kakao/varlog/pkg/types"
)

var appendBatchPool = sync.Pool{
	New: func() interface{} {
		return &AppendBatch{
			dk: make([]byte, dataKeyLength),
			ck: make([]byte, commitKeyLength),
			cc: make([]byte, commitContextLength),
		}
	},
}

// AppendBatch is a batch to put one or more log entries.
type AppendBatch struct {
	dataBatch   *pebble.Batch
	commitBatch *pebble.Batch
	writeOpts   *pebble.WriteOptions
	dk          []byte
	ck          []byte
	cc          []byte
}

func newAppendBatch(dataBatch, commitBatch *pebble.Batch, writeOpts *pebble.WriteOptions) *AppendBatch {
	ab := appendBatchPool.Get().(*AppendBatch)
	ab.dataBatch = dataBatch
	ab.commitBatch = commitBatch
	ab.writeOpts = writeOpts
	return ab
}

func (ab *AppendBatch) release() {
	ab.dataBatch = nil
	ab.commitBatch = nil
	ab.writeOpts = nil
	appendBatchPool.Put(ab)
}

// SetLogEntry inserts a log entry.
func (ab *AppendBatch) SetLogEntry(llsn types.LLSN, glsn types.GLSN, data []byte) error {
	dk := encodeDataKeyInternal(llsn, ab.dk)
	ck := encodeCommitKeyInternal(glsn, ab.ck)
	if err := ab.dataBatch.Set(dk, data, nil); err != nil {
		return err
	}
	if err := ab.commitBatch.Set(ck, dk, nil); err != nil {
		return err
	}
	return nil
}

// SetCommitContext inserts a commit context.
func (ab *AppendBatch) SetCommitContext(cc CommitContext) error {
	return ab.commitBatch.Set(commitContextKey, encodeCommitContext(cc, ab.cc), nil)
}

// Apply saves a batch of appended log entries to the storage.
func (ab *AppendBatch) Apply() error {
	if err := ab.dataBatch.Commit(ab.writeOpts); err != nil {
		return err
	}
	return ab.commitBatch.Commit(ab.writeOpts)
}

// Close releases an AppendBatch.
func (ab *AppendBatch) Close() error {
	err := errors.Join(ab.dataBatch.Close(), ab.commitBatch.Close())
	ab.release()
	return err
}
