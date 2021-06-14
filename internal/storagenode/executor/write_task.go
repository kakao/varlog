package executor

import (
	"sync"

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
}

// newWriteTask creates a new appendTask. The parameter twg should not be nil.
func newWriteTaskInternal(twg *taskWaitGroup, data []byte) *writeTask {
	if twg == nil {
		panic("twg is nil")
	}
	wt := writeTaskPool.Get().(*writeTask)
	wt.twg = twg
	wt.data = data
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
	writeTaskPool.Put(wt)
}
