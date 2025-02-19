package storage

import (
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/kakao/varlog/pkg/types"
)

var writeBatchPool = sync.Pool{
	New: func() interface{} {
		return &WriteBatch{
			dk: make([]byte, dataKeyLength),
		}
	},
}

// WriteBatch is a batch of writes to storage.
type WriteBatch struct {
	batch     *pebble.Batch
	writeOpts *pebble.WriteOptions
	dk        []byte
}

func newWriteBatch(batch *pebble.Batch, writeOpts *pebble.WriteOptions) *WriteBatch {
	wb := writeBatchPool.Get().(*WriteBatch)
	wb.batch = batch
	wb.writeOpts = writeOpts
	return wb
}

func (wb *WriteBatch) release() {
	wb.batch = nil
	wb.writeOpts = nil
	writeBatchPool.Put(wb)
}

// Set writes the given LLSN and data to the batch.
func (wb *WriteBatch) Set(llsn types.LLSN, data []byte) error {
	return wb.batch.Set(encodeDataKeyInternal(llsn, wb.dk), data, nil)
}

// Apply applies the batch to the underlying storage.
func (wb *WriteBatch) Apply() error {
	return wb.batch.Commit(wb.writeOpts)
}

// Close releases acquired resources.
func (wb *WriteBatch) Close() error {
	err := wb.batch.Close()
	wb.release()
	return err
}
