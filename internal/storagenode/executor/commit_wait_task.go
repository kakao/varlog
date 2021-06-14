package executor

import (
	"sync"

	"github.com/kakao/varlog/pkg/types"
)

var commitWaitTaskPool = sync.Pool{
	New: func() interface{} {
		t := &commitWaitTask{}
		return t
	},
}

// commitWaitTask is a task to stand by commit completes. It is instantiated by the writer and
// released by the committer.
type commitWaitTask struct {
	llsn types.LLSN
	twg  *taskWaitGroup
}

// newCommitWaitTask creates a new commitWaitTask. Parameter llsn should be valid. If parameter twg
// is nil, the committer won't notify commit completion. In the case of a backup replica, twg is
// usually nil.
func newCommitWaitTask(llsn types.LLSN, twg *taskWaitGroup) *commitWaitTask {
	if llsn.Invalid() {
		panic("invalid LLSN")
	}

	cwt := commitWaitTaskPool.Get().(*commitWaitTask)
	cwt.llsn = llsn
	cwt.twg = twg
	return cwt
}

func (cwt *commitWaitTask) release() {
	cwt.llsn = types.InvalidLLSN
	cwt.twg = nil
	commitWaitTaskPool.Put(cwt)
}
