package logstream

import (
	"sync"

	"github.com/kakao/varlog/proto/snpb"
)

var replicationTaskPool = sync.Pool{
	New: func() any {
		return &ReplicationTask{}
	},
}

type ReplicationTask struct {
	Req snpb.ReplicateRequest
	Err error
}

func NewReplicationTask() *ReplicationTask {
	return replicationTaskPool.Get().(*ReplicationTask)
}

func (rst *ReplicationTask) Release() {
	rst.Req.ResetReuse()
	rst.Err = nil
	replicationTaskPool.Put(rst)
}
