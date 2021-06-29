package executor

import (
	"context"
	"sync"
	"time"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
)

var replicateTaskPool = sync.Pool{
	New: func() interface{} {
		return &replicateTask{}
	},
}

type replicateTask struct {
	llsn     types.LLSN
	data     []byte
	replicas []snpb.Replica

	createdTime time.Time
	poppedTime  time.Time
}

func newReplicateTask() *replicateTask {
	rt := replicateTaskPool.Get().(*replicateTask)
	rt.createdTime = time.Now()
	return rt
}

func (t *replicateTask) release() {
	t.llsn = types.InvalidLLSN
	t.data = nil
	t.replicas = nil
	t.createdTime = time.Time{}
	t.poppedTime = time.Time{}
	replicateTaskPool.Put(t)
}

func (rt *replicateTask) annotate(ctx context.Context, m MeasurableExecutor) {
	if rt.createdTime.IsZero() || rt.poppedTime.IsZero() || !rt.poppedTime.After(rt.createdTime) {
		return
	}

	// write queue latency
	ms := float64(rt.poppedTime.Sub(rt.createdTime).Microseconds()) / 1000.0
	m.Stub().Metrics().ExecutorReplicateQueueTime.Record(ctx, ms)
}
