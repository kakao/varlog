package logstream

import (
	"sync"

	"github.com/kakao/varlog/internal/batchlet"
	"github.com/kakao/varlog/pkg/types"
)

// replicateTask is a task struct including a list of LLSNs and bytes of data.
type replicateTask struct {
	tpid     types.TopicID
	lsid     types.LogStreamID
	llsnList []types.LLSN
	dataList [][]byte

	poolIdx int
}

// newReplicateTask returns a new replicateTask.
// The argument poolIdx should be the index of replicateTaskPools, which is returned from batchlet.SelectLengthClass.
func newReplicateTask(poolIdx int) *replicateTask {
	rt := replicateTaskPools[poolIdx].Get().(*replicateTask)
	return rt
}

// release releases the task to the pool.
func (rt *replicateTask) release() {
	rt.tpid = 0
	rt.lsid = 0
	rt.llsnList = rt.llsnList[0:0]
	rt.dataList = nil
	replicateTaskPools[rt.poolIdx].Put(rt)
}

// releaseReplicateTasks releases all tasks in the list to the pool.
func releaseReplicateTasks(rts []*replicateTask) {
	for i := range rts {
		rts[i].release()
	}
}

var (
	replicateTaskPools = [...]sync.Pool{
		{
			New: func() interface{} {
				return &replicateTask{
					llsnList: make([]types.LLSN, 0, batchlet.LengthClasses[0]),
					poolIdx:  0,
				}
			},
		},
		{
			New: func() interface{} {
				return &replicateTask{
					llsnList: make([]types.LLSN, 0, batchlet.LengthClasses[1]),
					poolIdx:  1,
				}
			},
		},
		{
			New: func() interface{} {
				return &replicateTask{
					llsnList: make([]types.LLSN, 0, batchlet.LengthClasses[2]),
					poolIdx:  2,
				}
			},
		},
		{
			New: func() interface{} {
				return &replicateTask{
					llsnList: make([]types.LLSN, 0, batchlet.LengthClasses[3]),
					poolIdx:  3,
				}
			},
		},
	}
)

const defaultLengthOfReplicationTaskSlice = 3

var replicateTaskSlicePool = sync.Pool{
	New: func() interface{} {
		return make([]*replicateTask, 0, defaultLengthOfReplicationTaskSlice)
	},
}

func newReplicateTaskSlice() []*replicateTask {
	return replicateTaskSlicePool.Get().([]*replicateTask)
}

func releaseReplicateTaskSlice(rts []*replicateTask) {
	rts = rts[0:0]
	replicateTaskSlicePool.Put(rts) //nolint:staticcheck
}
