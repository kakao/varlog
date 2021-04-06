package executor

import (
	"sync"

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
}

func newReplicateTask() *replicateTask {
	return replicateTaskPool.Get().(*replicateTask)
}

func (t *replicateTask) release() {
	t.llsn = types.InvalidLLSN
	t.data = nil
	t.replicas = nil
	replicateTaskPool.Put(t)
}
