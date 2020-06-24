package storage

import (
	"fmt"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

type UncommittedLogStreamStatus struct {
	LogStreamID          types.LogStreamID
	KnownNextGLSN        types.GLSN
	UncommittedLLSNBegin types.LLSN
	UncommittedLLSNEnd   types.LLSN
}

type CommittedLogStreamStatus struct {
	LogStreamID        types.LogStreamID
	NextGLSN           types.GLSN
	PrevNextGLSN       types.GLSN
	CommittedGLSNBegin types.GLSN
	CommittedGLSNEnd   types.GLSN
}

type LogStreamReporter struct {
	storageNodeID types.StorageNodeID
	knownNextGLSN types.GLSN
	executors     map[types.LogStreamID]LogStreamExecutor
	mu            sync.RWMutex // guard for LogStreamExecutors
}

func NewLogStreamReporter(storageNodeID types.StorageNodeID) *LogStreamReporter {
	return &LogStreamReporter{
		storageNodeID: storageNodeID,
		knownNextGLSN: types.GLSN(0),
		executors:     make(map[types.LogStreamID]LogStreamExecutor),
	}
}

// RegisterLogStreamExecutor adds new LogStreamExecutor to LogStreamReporter. If a
// LogStreamReporter is added redundant, it returns varlog.ErrExist. If a given LogStreamExecutor
// is nil, it returns varlog.ErrInvalid.
// When the new LogStreamExecutor is registered successfully, it is received a GLSN which is
// anticipated to be issued in the next commit - knownNextGLSN.
func (r *LogStreamReporter) RegisterLogStreamExecutor(logStreamID types.LogStreamID, executor LogStreamExecutor) error {
	if executor == nil {
		return varlog.ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.executors[logStreamID]
	if ok {
		return varlog.ErrExist
	}
	r.executors[logStreamID] = executor
	return nil
}

// GetReport collects statuses about uncommitted log entries from log streams in the storage node.
// KnownNextGLSNs from all LogStreamExecutors must be equal to the corresponding in
// LogStreamReporter.
func (r *LogStreamReporter) GetReport() (types.GLSN, []UncommittedLogStreamStatus) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	reports := make([]UncommittedLogStreamStatus, len(r.executors))
	i := 0
	for _, executor := range r.executors {
		status := executor.GetLogStreamStatus()
		reports[i] = status
		i++
		// FIXME: invariant for KnownNextGLSN
		if status.KnownNextGLSN == 0 {
			// New LogStreamExecutor has no idea KnownNextGLSN.
			continue
		}
		if r.knownNextGLSN != status.KnownNextGLSN {
			panic("KnownNextGLSN mismatch")
		}

	}
	return r.knownNextGLSN, reports
}

func (r *LogStreamReporter) Commit(nextGLSN, prevNextGLSN types.GLSN, commitResults []CommittedLogStreamStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if prevNextGLSN < r.knownNextGLSN {
		// stale commit result
		return
	}
	if prevNextGLSN > r.knownNextGLSN {
		panic("missed something")
	}
	r.knownNextGLSN = nextGLSN
	for _, commitResult := range commitResults {
		logStreamID := commitResult.LogStreamID
		executor, ok := r.executors[logStreamID]
		if !ok {
			panic(fmt.Sprintf("no such LogStreamExecutor in this StorageNode: %d", logStreamID))
		}
		executor.(LogStreamExecutor).CommitLogStreamStatusResult(commitResult)
	}
}
