package executor

import (
	"context"
	"sync"
	"time"

	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

var writeTaskPool = sync.Pool{
	New: func() interface{} {
		t := &writeTask{}
		return t
	},
}

// writeTask is a task that represents appending and replicating.
// It is managed by writeTaskPool and acquired by the Append or the Replicate RPC handlers.
// It is sent to the writer through internal/storagenode/executor.writeQueue.
// Note that the writer releases the writeTask that is either written to disk or failed.
type writeTask struct {
	llsn types.LLSN
	data []byte

	// backups is a list of backups of the log stream. The first element is the primary
	// replica, and the others are backup backups.
	backups []varlogpb.Replica

	// NOTE: primary can be removed by using isPrimary method of executor.
	primary bool

	twg *taskWaitGroup

	validate func() error

	// variables for metrics
	createdTime    time.Time
	poppedTime     time.Time
	processingTime time.Time
}

// newWriteTask creates a new appendTask. The parameter twg should not be nil.
func newWriteTaskInternal(twg *taskWaitGroup, data []byte) *writeTask {
	wt := writeTaskPool.Get().(*writeTask)
	wt.twg = twg
	wt.data = data
	wt.createdTime = time.Now()
	return wt
}

// newPrimaryWriteTask creates a new writeTask to be used in a primary replica.
func newPrimaryWriteTask(twg *taskWaitGroup, data []byte, backups []varlogpb.Replica) *writeTask {
	if twg == nil {
		panic("twg is nil")
	}
	wt := newWriteTaskInternal(twg, data)
	wt.primary = true
	wt.backups = backups
	return wt
}

// newBackupWriteTask creates a new writeTask to be used in a backup replica.
//
// Field llsn should be valid since replicated data must have a position in the log stream.
// Field twg is nil since the Replicate RPC in the backup replica must not wait for the data to be committed.
//
// Even, the Replicate RPC does not wait for data to be written into a disk, thus releasing the
// writeTask in the backup replica is the responsibility of the
// `internal/storagenode/executor.(writer)`.
func newBackupWriteTask(data []byte, llsn types.LLSN) *writeTask {
	if llsn.Invalid() {
		panic("invalid LLSN")
	}
	wt := newWriteTaskInternal(nil, data)
	wt.llsn = llsn
	wt.primary = false
	wt.backups = nil
	return wt
}

func (wt *writeTask) release() {
	wt.llsn = types.InvalidLLSN
	wt.data = nil
	wt.backups = nil
	wt.validate = nil
	wt.twg = nil
	wt.createdTime = time.Time{}
	wt.poppedTime = time.Time{}
	wt.processingTime = time.Time{}
	writeTaskPool.Put(wt)
}

func (wt *writeTask) annotate(ctx context.Context, m *telemetry.Metrics) {
	if wt.createdTime.IsZero() || wt.poppedTime.IsZero() || !wt.poppedTime.After(wt.createdTime) {
		return
	}

	// write queue latency
	ms := float64(wt.poppedTime.Sub(wt.createdTime).Microseconds()) / 1000.0
	m.WriteQueueTime.Record(ctx, ms)

	// processing time
	if wt.processingTime.IsZero() || !wt.processingTime.After(wt.poppedTime) {
		return
	}
	ms = float64(wt.processingTime.Sub(wt.poppedTime).Microseconds()) / 1000.0
	m.WriteTime.Record(ctx, ms)
}
