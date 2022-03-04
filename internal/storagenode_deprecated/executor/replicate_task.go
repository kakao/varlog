package executor

import (
	"context"
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/telemetry"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

var replicateTaskPool = sync.Pool{
	New: func() interface{} {
		return &replicateTask{}
	},
}

type replicateTask struct {
	llsn     types.LLSN
	data     []byte
	replicas []varlogpb.Replica

	createdTime time.Time
	poppedTime  time.Time
}

func newReplicateTask() *replicateTask {
	rt := replicateTaskPool.Get().(*replicateTask)
	rt.createdTime = time.Now()
	return rt
}

func (rt *replicateTask) release() {
	rt.llsn = types.InvalidLLSN
	rt.data = nil
	rt.replicas = nil
	rt.createdTime = time.Time{}
	rt.poppedTime = time.Time{}
	replicateTaskPool.Put(rt)
}

func (rt *replicateTask) annotate(ctx context.Context, m *telemetry.Metrics) {
	if rt.createdTime.IsZero() || rt.poppedTime.IsZero() || !rt.poppedTime.After(rt.createdTime) {
		return
	}

	// write queue latency
	ms := float64(rt.poppedTime.Sub(rt.createdTime).Microseconds()) / 1000.0
	m.ReplicateQueueTime.Record(ctx, ms)
}
