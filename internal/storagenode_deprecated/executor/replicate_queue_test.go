package executor

/*
type mockReplicateQueue struct {
	mock.Mock
	numItems int64
}

var _ replicateQueue = (*mockReplicateQueue)(nil)

func (rq *mockReplicateQueue) push(ctx context.Context, replicateTaskBlocks ...*replicateTaskBlock) error {
	atomic.AddInt64(&rq.numItems, 1)
	args := rq.Called(ctx, replicateTaskBlocks)
	return args.Error(0)
}

func (rq *mockReplicateQueue) pop(ctx context.Context) (*replicateTaskBlock, error) {
	atomic.AddInt64(&rq.numItems, -1)
	args := rq.Called(ctx)
	return args.Get(0).(*replicateTaskBlock), args.Error(1)
}

func (rq *mockReplicateQueue) size() int {
	ret := atomic.LoadInt64(&rq.numItems)
	return int(ret)
}

func (rq *mockReplicateQueue) close() {
}

func (rq *mockReplicateQueue) drain(drainFunc func(*replicateTaskBlock)) {
}
*/
