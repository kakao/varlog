package replication

import (
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

var replicateTaskPool = sync.Pool{
	New: func() interface{} {
		return &replicateTask{}
	},
}

type replicateTask struct {
	req            snpb.ReplicationRequest
	err            error
	createdTime    time.Time
	replicatedTime time.Time
}

func newReplicateTask() *replicateTask {
	rt := replicateTaskPool.Get().(*replicateTask)
	rt.createdTime = time.Now()
	return rt
}

func (rt *replicateTask) release() {
	rt.req = snpb.ReplicationRequest{}
	rt.err = nil
	rt.createdTime = time.Time{}
	rt.replicatedTime = time.Time{}
	replicateTaskPool.Put(rt)
}
