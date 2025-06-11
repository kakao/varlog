package storage

import (
	"sync"

	"github.com/cockroachdb/pebble/v2"

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
	s         *store
}

func newWriteBatch(s *store) *WriteBatch {
	wb := writeBatchPool.Get().(*WriteBatch)
	wb.batch = s.db.NewBatch()
	wb.writeOpts = s.writeOpts
	wb.s = s
	return wb
}

func (wb *WriteBatch) release() {
	wb.batch = nil
	wb.writeOpts = nil
	wb.s = nil
	writeBatchPool.Put(wb)
}

// Set writes the given LLSN and data to the batch.
func (wb *WriteBatch) Set(llsn types.LLSN, data []byte) error {
	return wb.batch.Set(encodeDataKeyInternal(llsn, wb.dk), data, nil)
}

// Apply applies the batch to the underlying storage.
func (wb *WriteBatch) Apply() error {
	if err := wb.batch.Commit(wb.writeOpts); err != nil {
		return err
	}
	if wb.s.telemetryConfig.enable {
		wb.s.telemetryConfig.metricRecorder.RecordBatchCommitStats(todoContext, BatchCommitStats{wb.batch.CommitStats()})
	}
	return nil
}

// Close releases acquired resources.
func (wb *WriteBatch) Close() error {
	err := wb.batch.Close()
	wb.release()
	return err
}
