package executor

import (
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

var commitTaskPool = sync.Pool{
	New: func() interface{} {
		return &commitTask{}
	},
}

type commitTask struct {
	highWatermark      types.GLSN
	prevHighWatermark  types.GLSN
	committedGLSNBegin types.GLSN
	committedGLSNEnd   types.GLSN
	committedLLSNBegin types.LLSN

	ctime time.Time
}

func newCommitTask() *commitTask {
	return commitTaskPool.Get().(*commitTask)
}

func (t *commitTask) release() {
	t.highWatermark = types.InvalidGLSN
	t.prevHighWatermark = types.InvalidGLSN
	t.committedGLSNBegin = types.InvalidGLSN
	t.committedGLSNEnd = types.InvalidGLSN
	t.committedLLSNBegin = types.InvalidLLSN
	commitTaskPool.Put(t)
}
