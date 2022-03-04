package executor

import (
	"context"
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/telemetry"
	"github.daumkakao.com/varlog/varlog/pkg/types"
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

	createdTime    time.Time
	poppedTime     time.Time
	processingTime time.Time
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
	cwt.createdTime = time.Now()
	return cwt
}

func (cwt *commitWaitTask) release() {
	cwt.llsn = types.InvalidLLSN
	cwt.twg = nil
	cwt.createdTime = time.Time{}
	cwt.poppedTime = time.Time{}
	cwt.processingTime = time.Time{}
	commitWaitTaskPool.Put(cwt)
}

func (cwt *commitWaitTask) annotate(ctx context.Context, m *telemetry.Metrics) {
	if cwt.createdTime.IsZero() || cwt.poppedTime.IsZero() || !cwt.poppedTime.After(cwt.createdTime) {
		return
	}

	// queue latency
	ms := float64(cwt.poppedTime.Sub(cwt.createdTime).Microseconds()) / 1000.0
	m.CommitWaitQueueTime.Record(ctx, ms)
}
