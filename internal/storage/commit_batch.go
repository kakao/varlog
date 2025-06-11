package storage

import (
	"sync"

	"github.com/cockroachdb/pebble/v2"

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
	s         *store
}

func newCommitBatch(s *store) *CommitBatch {
	cb := commitBatchPool.Get().(*CommitBatch)
	cb.batch = s.db.NewBatch()
	cb.writeOpts = s.writeOpts
	cb.s = s
	return cb
}

func (cb *CommitBatch) release() {
	cb.batch = nil
	cb.writeOpts = nil
	cb.s = nil
	commitBatchPool.Put(cb)
}

func (cb *CommitBatch) Set(llsn types.LLSN, glsn types.GLSN) error {
	return cb.batch.Set(encodeCommitKeyInternal(glsn, cb.ck), encodeDataKeyInternal(llsn, cb.dk), nil)
}

func (cb *CommitBatch) Apply() error {
	if err := cb.batch.Commit(cb.writeOpts); err != nil {
		return err
	}
	if cb.s.telemetryConfig.enable {
		cb.s.telemetryConfig.metricRecorder.RecordBatchCommitStats(todoContext, BatchCommitStats{cb.batch.CommitStats()})
	}
	return nil
}

func (cb *CommitBatch) Close() error {
	err := cb.batch.Close()
	cb.release()
	return err
}
