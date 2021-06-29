package executor

import (
	"context"
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

var writeTaskPool = sync.Pool{
	New: func() interface{} {
		t := &writeTask{}
		return t
	},
}

// writeTask represents either append or replicate. In the Append or the Replicate RPC handlers, an
// writeTask is created.
type writeTask struct {
	llsn types.LLSN
	data []byte

	// backups is a list of backups of the log stream. The first element is the primary
	// replica, and the others are backup backups.
	backups []snpb.Replica

	// NOTE: primary can be removed by using isPrimary method of executor.
	primary bool

	twg *taskWaitGroup

	validate func() error

	createdTime    time.Time
	poppedTime     time.Time
	processingTime time.Time
}

// newWriteTask creates a new appendTask. The parameter twg should not be nil.
func newWriteTaskInternal(twg *taskWaitGroup, data []byte) *writeTask {
	if twg == nil {
		panic("twg is nil")
	}
	wt := writeTaskPool.Get().(*writeTask)
	wt.twg = twg
	wt.data = data
	wt.createdTime = time.Now()
	return wt
}

func newPrimaryWriteTask(twg *taskWaitGroup, data []byte, backups []snpb.Replica) *writeTask {
	wt := newWriteTaskInternal(twg, data)
	wt.primary = true
	wt.backups = backups
	return wt

}
func newBackupWriteTask(twg *taskWaitGroup, data []byte, llsn types.LLSN) *writeTask {
	if llsn.Invalid() {
		panic("invalid LLSN")
	}
	wt := newWriteTaskInternal(twg, data)
	wt.llsn = llsn
	wt.primary = false
	wt.backups = nil
	return wt
}

func (wt *writeTask) release() {
	wt.llsn = types.InvalidLLSN
	wt.data = nil
	wt.backups = nil
	// wt.primary = false
	wt.validate = nil
	wt.twg = nil
	wt.createdTime = time.Time{}
	wt.poppedTime = time.Time{}
	wt.processingTime = time.Time{}
	writeTaskPool.Put(wt)
}

func (wt *writeTask) annotate(ctx context.Context, m MeasurableExecutor) {
	if wt.createdTime.IsZero() || wt.poppedTime.IsZero() || !wt.poppedTime.After(wt.createdTime) {
		return
	}

	// write queue latency
	ms := float64(wt.poppedTime.Sub(wt.createdTime).Microseconds()) / 1000.0
	m.Stub().Metrics().ExecutorWriteQueueTime.Record(ctx, ms)

	// processing time
	if wt.processingTime.IsZero() || !wt.processingTime.After(wt.poppedTime) {
		return
	}
	ms = float64(wt.processingTime.Sub(wt.poppedTime).Microseconds()) / 1000.0
	m.Stub().Metrics().ExecutorWriteTime.Record(ctx, ms)
}
