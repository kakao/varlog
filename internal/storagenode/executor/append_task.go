package executor

import (
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

var appendTaskPool = sync.Pool{
	New: func() interface{} {
		t := &appendTask{}
		return t
	},
}

type appendTask struct {
	llsn types.LLSN
	glsn types.GLSN
	data []byte

	// replicas is a list of replicas of the log stream. The first element is the primary
	// replica, and the others are backup replicas.
	replicas []snpb.Replica

	primary bool

	wg  sync.WaitGroup
	err error

	validate func() error
}

func newAppendTask() *appendTask {
	task := appendTaskPool.Get().(*appendTask)
	return task
}

func (t *appendTask) release() {
	t.llsn = types.InvalidLLSN
	t.glsn = types.InvalidGLSN
	t.data = nil
	t.replicas = nil
	t.err = nil
	t.primary = false
	t.validate = nil
	appendTaskPool.Put(t)
}

func (t *appendTask) clone() *appendTask {
	clone := newAppendTask()
	clone.llsn = t.llsn
	clone.glsn = t.glsn
	copy(clone.data, t.data)
	clone.replicas = make([]snpb.Replica, len(t.replicas))
	for i := 0; i < len(clone.replicas); i++ {
		clone.replicas[i] = t.replicas[i]
	}
	return clone
}

func newAppendTaskBatchPool(batchSize int) *sync.Pool {
	pool := &sync.Pool{}
	pool.New = func() interface{} {
		return &appendTaskBatch{
			batch: make([]*appendTask, 0, batchSize),
			pool:  pool,
		}
	}
	return pool
}

type appendTaskBatch struct {
	batch []*appendTask
	pool  *sync.Pool
}

func newAppendTaskBatch(pool *sync.Pool) *appendTaskBatch {
	return pool.Get().(*appendTaskBatch)
}

func (b *appendTaskBatch) release() {
	b.batch = b.batch[0:0]
	b.pool.Put(b)
}
