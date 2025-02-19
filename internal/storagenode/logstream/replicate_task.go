package logstream

import (
	"sync"

	"github.com/kakao/varlog/pkg/types"
)

// replicateTask is a task struct including a list of LLSNs and bytes of data.
type replicateTask struct {
	tpid     types.TopicID
	lsid     types.LogStreamID
	llsnList []types.LLSN
	dataList [][]byte
}

// newReplicateTask returns a new replicateTask. The capacity of the returned
// replicateTask's llsnList is equal to or greater than the argument size, and
// its length is zero.
// Since (snpb.ReplicateRequest).LLSN is deprecated, (*replicateTask).llsnList
// will be deprecated soon. Until that, newReplicateTask simplifies the pool
// management of replicateTask.
func newReplicateTask(size int) *replicateTask {
	return defaultReplicateTaskPool.get(size)
}

// release relreases the task to the pool.
func (rt *replicateTask) release() {
	rt.tpid = 0
	rt.lsid = 0
	rt.llsnList = rt.llsnList[0:0]
	rt.dataList = nil
	defaultReplicateTaskPool.put(rt)
}

// releaseReplicateTasks releases all tasks in the list to the pool.
func releaseReplicateTasks(rts []*replicateTask) {
	for i := range rts {
		rts[i].release()
	}
}

// replicateTaskPool is a simple pool for replicateTask.
type replicateTaskPool struct {
	pool sync.Pool
}

var defaultReplicateTaskPool replicateTaskPool

func (p *replicateTaskPool) get(size int) *replicateTask {
	rt, ok := p.pool.Get().(*replicateTask)
	if ok && cap(rt.llsnList) >= size {
		rt.llsnList = rt.llsnList[0:0]
		return rt
	}
	if ok {
		p.pool.Put(rt)
	}
	return &replicateTask{
		llsnList: make([]types.LLSN, 0, size),
	}
}

func (p *replicateTaskPool) put(rt *replicateTask) {
	p.pool.Put(rt)
}

const defaultLengthOfReplicationTaskSlice = 3

type replicateTaskSlice struct {
	tasks []*replicateTask
}

var replicateTaskSlicePool = sync.Pool{
	New: func() interface{} {
		return &replicateTaskSlice{
			tasks: make([]*replicateTask, 0, defaultLengthOfReplicationTaskSlice),
		}
	},
}

func newReplicateTaskSlice() *replicateTaskSlice {
	return replicateTaskSlicePool.Get().(*replicateTaskSlice)
}

func releaseReplicateTaskSlice(rts *replicateTaskSlice) {
	rts.tasks = rts.tasks[0:0]
	replicateTaskSlicePool.Put(rts)
}
