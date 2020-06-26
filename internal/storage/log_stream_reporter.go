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
	knownNextGLSN types.AtomicGLSN
	executors     map[types.LogStreamID]LogStreamExecutor
	mtxExecutors  sync.RWMutex
	history       map[types.GLSN][]UncommittedLogStreamStatus
	mtxHistory    sync.RWMutex
	mtxCommit     sync.Mutex
}

func NewLogStreamReporter(storageNodeID types.StorageNodeID) *LogStreamReporter {
	return &LogStreamReporter{
		storageNodeID: storageNodeID,
		executors:     make(map[types.LogStreamID]LogStreamExecutor),
		history:       make(map[types.GLSN][]UncommittedLogStreamStatus),
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
	r.mtxExecutors.Lock()
	defer r.mtxExecutors.Unlock()
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
	r.mtxExecutors.RLock()
	defer r.mtxExecutors.RUnlock()
	reports := make([]UncommittedLogStreamStatus, len(r.executors))
	i := 0
	minKnownNextGLSN := types.GLSN(0)
	for _, executor := range r.executors {
		status := executor.GetReport()
		reports[i] = status
		if status.KnownNextGLSN == 0 {
			// newbie: New LogStreamExecutor has no idea KnownNextGLSN.
			continue
		}
		if minKnownNextGLSN == 0 || minKnownNextGLSN > status.KnownNextGLSN {
			minKnownNextGLSN = status.KnownNextGLSN
		}
	}
	knownNextGLSN := r.knownNextGLSN.Load()
	if minKnownNextGLSN < knownNextGLSN {
		// slow LSE
		// get the past report from history map
		// use minKnownNextGLSN as return value
		knownNextGLSN = minKnownNextGLSN
		r.mtxHistory.RLock()
		if rpt, ok := r.history[knownNextGLSN]; ok {
			reports = rpt
		}
		r.mtxHistory.RUnlock()
	}
	r.mtxHistory.Lock()
	defer r.mtxHistory.Unlock()
	if _, ok := r.history[knownNextGLSN]; !ok {
		r.history[knownNextGLSN] = reports
	}
	// NOTE: history map will be small - most 2 elements
	// TODO: remove this after implementing log stream-wise report
	for nextGLSN, _ := range r.history {
		if nextGLSN < knownNextGLSN {
			delete(r.history, nextGLSN)
		}
	}
	return knownNextGLSN, reports
}

func (r *LogStreamReporter) Commit(nextGLSN, prevNextGLSN types.GLSN, commitResults []CommittedLogStreamStatus) {
	r.mtxCommit.Lock()
	defer r.mtxCommit.Unlock()

	knownNextGLSN := r.knownNextGLSN.Load()
	if prevNextGLSN < knownNextGLSN {
		// TODO: stale commit result
		return
	}
	if prevNextGLSN > knownNextGLSN {
		// TODO: missed something - not yet ready to commit it
		return
	}
	for _, commitResult := range commitResults {
		logStreamID := commitResult.LogStreamID
		r.mtxExecutors.RLock()
		executor, ok := r.executors[logStreamID]
		r.mtxExecutors.RUnlock()
		if !ok {
			panic(fmt.Sprintf("no such LogStreamExecutor in this StorageNode: %d", logStreamID))
		}
		executor.Commit(commitResult)
	}
	r.knownNextGLSN.Store(nextGLSN)
}
