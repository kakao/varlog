package executor

import "sync"

type commitWaitTaskBatchPool struct {
	batchSize int
	pool      sync.Pool
}

func newCommitWaitTaskBatchPool(batchSize int) *commitWaitTaskBatchPool {
	return &commitWaitTaskBatchPool{
		batchSize: batchSize,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]*commitWaitTask, 0, batchSize)
			},
		},
	}
}

func (p *commitWaitTaskBatchPool) new() []*commitWaitTask {
	return p.pool.Get().([]*commitWaitTask)
}

func (p *commitWaitTaskBatchPool) release(batch []*commitWaitTask) {
	batch = batch[0:0]
	p.pool.Put(batch)
}

type replicateTaskBatchPool struct {
	batchSize int
	pool      sync.Pool
}

func newReplicateTaskBatchPool(batchSize int) *replicateTaskBatchPool {
	return &replicateTaskBatchPool{
		batchSize: batchSize,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]*replicateTask, 0, batchSize)
			},
		},
	}
}

func (p *replicateTaskBatchPool) new() []*replicateTask {
	return p.pool.Get().([]*replicateTask)
}

func (p *replicateTaskBatchPool) release(batch []*replicateTask) {
	batch = batch[0:0]
	p.pool.Put(batch)
}
