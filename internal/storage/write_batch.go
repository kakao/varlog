package storage

import (
	"sync"

	"github.com/cockroachdb/pebble"

	"github.daumkakao.com/varlog/varlog/pkg/types"
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

/*
// Deferred returns a new DeferredWriteBatch that can be used to defer assigning LLSNs.
func (wb *WriteBatch) Deferred(poolIdx int) *DeferredWriteBatch {
	if wb.count != 0 {
		panic("storage: non empty write batch could not be deferred")
	}
	dwb := newDeferredWriteBatch(wb, poolIdx)
	return dwb
}
*/

// Put writes the given LLSN and data to the batch.
//func (wb *WriteBatch) Put(llsn types.LLSN, data []byte) error {
//	dk := encodeDataKeyInternal(llsn, wb.dk)
//	if err := wb.batch.Set(dk, data, nil); err != nil {
//		return err
//	}
//	return nil
//}

// Set writes the given LLSN and data to the batch.
func (wb *WriteBatch) Set(llsn types.LLSN, data []byte) error {
	return wb.batch.Set(encodeDataKeyInternal(llsn, wb.dk), data, nil)
}

// SetDeferred writes the given LLSN and data to the batch.
//func (wb *WriteBatch) SetDeferred(llsn types.LLSN, data []byte) error {
//	op := wb.batch.SetDeferred(dataKeyLength, len(data))
//	op.Key = encodeDataKeyInternal(llsn, op.Key)
//	copy(op.Value, data)
//	op.Finish()
//	return nil
//}

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

/*
var deferredWriteBatchPools = [...]sync.Pool{
	{
		New: func() interface{} {
			return &DeferredWriteBatch{
				deferredOps: make([]*pebble.DeferredBatchOp, 0, batchlet.LengthClasses[0]),
				poolIdx:     0,
			}
		},
	},
	{
		New: func() interface{} {
			return &DeferredWriteBatch{
				deferredOps: make([]*pebble.DeferredBatchOp, 0, batchlet.LengthClasses[1]),
				poolIdx:     1,
			}
		},
	},
	{
		New: func() interface{} {
			return &DeferredWriteBatch{
				deferredOps: make([]*pebble.DeferredBatchOp, 0, batchlet.LengthClasses[2]),
				poolIdx:     2,
			}
		},
	},
	{
		New: func() interface{} {
			return &DeferredWriteBatch{
				deferredOps: make([]*pebble.DeferredBatchOp, 0, batchlet.LengthClasses[3]),
				poolIdx:     3,
			}
		},
	},
}

type DeferredWriteBatch struct {
	wb          *WriteBatch
	deferredOps []*pebble.DeferredBatchOp
	poolIdx     int
}

func newDeferredWriteBatch(wb *WriteBatch, poolIdx int) *DeferredWriteBatch {
	dwb := deferredWriteBatchPools[poolIdx].Get().(*DeferredWriteBatch)
	dwb.wb = wb
	return dwb
}

func (dwb *DeferredWriteBatch) release() {
	dwb.wb = nil
	dwb.deferredOps = dwb.deferredOps[0:0]
	deferredWriteBatchPools[dwb.poolIdx].Put(dwb)
}

func (dwb *DeferredWriteBatch) PutData(data []byte) {
	deferredOp := dwb.wb.batch.SetDeferred(dataKeyLength, len(data))
	copy(deferredOp.Value, data)
	dwb.deferredOps = append(dwb.deferredOps, deferredOp)
}

func (dwb *DeferredWriteBatch) SetLLSN(idx int, llsn types.LLSN) {
	dwb.deferredOps[idx].Key = encodeDataKeyInternal(llsn, dwb.deferredOps[idx].Key)
	dwb.deferredOps[idx].Finish()
	dwb.deferredOps[idx] = nil
}

func (dwb *DeferredWriteBatch) Apply() error {
	return dwb.wb.Apply()
}

func (dwb *DeferredWriteBatch) Close() error {
	err := dwb.wb.Close()
	dwb.release()
	return err
}
*/
