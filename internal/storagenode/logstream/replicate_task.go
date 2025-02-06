package logstream

import (
	"sync"

	"github.com/kakao/varlog/pkg/types"
)

// replicateTask is a task struct including a list of LLSNs and bytes of data.
type replicateTask struct {
	tpid      types.TopicID
	lsid      types.LogStreamID
	beginLLSN types.LLSN
	dataList  [][]byte
}

// newReplicateTask returns a new replicateTask.
func newReplicateTask() *replicateTask {
	return defaultReplicateTaskPool.get()
}

// release relreases the task to the pool.
func (rt *replicateTask) release() {
	*rt = replicateTask{}
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

func (p *replicateTaskPool) get() *replicateTask {
	rt, ok := p.pool.Get().(*replicateTask)
	if ok {
		return rt
	}
	return &replicateTask{}
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
