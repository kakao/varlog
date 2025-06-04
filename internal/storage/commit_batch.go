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
	batch          *pebble.Batch
	writeOpts      *pebble.WriteOptions
	cc             []byte
	ck             []byte
	dk             []byte
	metricRecorder MetricRecorder
}

func newCommitBatch(s *store) *CommitBatch {
	cb := commitBatchPool.Get().(*CommitBatch)
	cb.batch = s.db.NewBatch()
	cb.writeOpts = s.writeOpts
	cb.metricRecorder = s.metricRecorder
	return cb
}

func (cb *CommitBatch) release() {
	cb.batch = nil
	cb.writeOpts = nil
	cb.metricRecorder = nil
	commitBatchPool.Put(cb)
}

func (cb *CommitBatch) Set(llsn types.LLSN, glsn types.GLSN) error {
	return cb.batch.Set(encodeCommitKeyInternal(glsn, cb.ck), encodeDataKeyInternal(llsn, cb.dk), nil)
}

func (cb *CommitBatch) Apply() error {
	if err := cb.batch.Commit(cb.writeOpts); err != nil {
		return err
	}
	cb.metricRecorder.RecordBatchCommitStats(todoContext, BatchCommitStats{cb.batch.CommitStats()})
	return nil
}

func (cb *CommitBatch) Close() error {
	err := cb.batch.Close()
	cb.release()
	return err
}
