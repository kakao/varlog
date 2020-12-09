package storagenode

import (
	"sync"

	"github.com/kakao/varlog/pkg/types"
)

// reportCommitContext represents context containing information that should be consistent during
// the same commit group and report to the metadata repository.
//
// globalHighwatermark acts as a version of commit results. In the same globalHighwatermark,
// all the reports to the metadata repository should have the same uncommittedLLSNBegin and
// vice versa.
//
// globalHighwatermark: read by GetReport / written by Commit and SyncReplicate
// uncommittedLLSNBegin: read by GetReport / written by Commit and SyncReplicate
type reportCommitContext struct {
	globalHighwatermark  types.GLSN
	uncommittedLLSNBegin types.LLSN
	mu                   sync.RWMutex
}

func (rcc *reportCommitContext) get() (globalHighwatermark types.GLSN, uncommittedLLSNBegin types.LLSN) {
	rcc.mu.RLock()
	defer rcc.mu.RUnlock()
	return rcc.globalHighwatermark, rcc.uncommittedLLSNBegin
}

// logStreamContext represents context containing runtime information of log stream executor. The
// log stream executor should save its runtime to persistent storage to recover its last status
// after restarting it.
//
// Members of logStreamContext are read and written by some methods of log stream executor. They are
// also recovered by the initialization of storage or sync replication.
type logStreamContext struct {
	// See reportCommitContext.
	rcc reportCommitContext

	// committedLLSNEnd is the next position to be committed to storage. It can increases by
	// commit.
	committedLLSNEnd types.LLSN

	// uncommittedLLSNEnd is the next position to be written. It is increased after successful
	// writing to the storage during the append operation.
	uncommittedLLSNEnd types.AtomicLLSN

	// localLowWatermark is the lowest limit to be able to read logs. It can increase by trim
	localLowWatermark types.AtomicGLSN

	// localHighWatermark is the highest limit to be able to read logs. It can increases by
	// commit
	localHighWatermark types.AtomicGLSN
}

func initLogStreamContext(lsc *logStreamContext) {
	lsc.rcc.globalHighwatermark = types.InvalidGLSN
	lsc.rcc.uncommittedLLSNBegin = types.MinLLSN
	lsc.uncommittedLLSNEnd.Store(types.MinLLSN)
	lsc.committedLLSNEnd = types.MinLLSN
	lsc.localLowWatermark.Store(types.MinGLSN)
	lsc.localHighWatermark.Store(types.InvalidGLSN)
}
