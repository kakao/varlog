package storage

import (
	"context"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/runner"
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

const lsrCommitCSize = 0

type lsrCommitTask struct {
	nextGLSN      types.GLSN
	prevNextGLSN  types.GLSN
	commitResults []CommittedLogStreamStatus
}

type LogStreamReporter interface {
	Run(ctx context.Context)
	Close()
	StorageNodeID() types.StorageNodeID
	RegisterLogStreamExecutor(executor LogStreamExecutor) error
	GetReport() (types.GLSN, []UncommittedLogStreamStatus)
	Commit(nextGLSN, prevNextGLSN types.GLSN, commitResults []CommittedLogStreamStatus)
}

type logStreamReporter struct {
	storageNodeID types.StorageNodeID
	knownNextGLSN types.AtomicGLSN
	executors     map[types.LogStreamID]LogStreamExecutor
	mtxExecutors  sync.RWMutex
	history       map[types.GLSN][]UncommittedLogStreamStatus
	mtxHistory    sync.RWMutex
	commitC       chan lsrCommitTask
	cancel        context.CancelFunc
	runner        runner.Runner
	once          sync.Once
}

func NewLogStreamReporter(storageNodeID types.StorageNodeID) LogStreamReporter {
	return &logStreamReporter{
		storageNodeID: storageNodeID,
		executors:     make(map[types.LogStreamID]LogStreamExecutor),
		history:       make(map[types.GLSN][]UncommittedLogStreamStatus),
		commitC:       make(chan lsrCommitTask, lsrCommitCSize),
	}
}

func (lsr *logStreamReporter) StorageNodeID() types.StorageNodeID {
	return lsr.storageNodeID
}

func (lsr *logStreamReporter) Run(ctx context.Context) {
	lsr.once.Do(func() {
		ctx, lsr.cancel = context.WithCancel(ctx)
		lsr.runner.Run(ctx, lsr.dispatchCommit)
	})
}

func (lsr *logStreamReporter) Close() {
	if lsr.cancel != nil {
		lsr.cancel()
		lsr.runner.CloseWait()
	}
}

func (lsr *logStreamReporter) dispatchCommit(ctx context.Context) {
	for {
		select {
		case t := <-lsr.commitC:
			lsr.commit(t)
		case <-ctx.Done():
			return
		}
	}
}

// RegisterLogStreamExecutor adds new LogStreamExecutor to LogStreamReporter. If a
// LogStreamReporter is added redundant, it returns varlog.ErrExist. If a given LogStreamExecutor
// is nil, it returns varlog.ErrInvalid.
// When the new LogStreamExecutor is registered successfully, it is received a GLSN which is
// anticipated to be issued in the next commit - knownNextGLSN.
func (lsr *logStreamReporter) RegisterLogStreamExecutor(executor LogStreamExecutor) error {
	if executor == nil {
		return varlog.ErrInvalid
	}
	logStreamID := executor.LogStreamID()
	lsr.mtxExecutors.Lock()
	defer lsr.mtxExecutors.Unlock()
	_, ok := lsr.executors[logStreamID]
	if ok {
		return varlog.ErrExist
	}
	lsr.executors[logStreamID] = executor
	return nil
}

// GetReport collects statuses about uncommitted log entries from log streams in the storage node.
// KnownNextGLSNs from all LogStreamExecutors must be equal to the corresponding in
// LogStreamReporter.
func (lsr *logStreamReporter) GetReport() (types.GLSN, []UncommittedLogStreamStatus) {
	lsr.mtxExecutors.RLock()
	defer lsr.mtxExecutors.RUnlock()
	reports := make([]UncommittedLogStreamStatus, len(lsr.executors))
	i := 0
	minKnownNextGLSN := types.GLSN(0)
	for _, executor := range lsr.executors {
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
	knownNextGLSN := lsr.knownNextGLSN.Load()
	if minKnownNextGLSN < knownNextGLSN {
		// slow LSE
		// get the past report from history map
		// use minKnownNextGLSN as return value
		knownNextGLSN = minKnownNextGLSN
		lsr.mtxHistory.RLock()
		if rpt, ok := lsr.history[knownNextGLSN]; ok {
			reports = rpt
		}
		lsr.mtxHistory.RUnlock()
	}
	lsr.mtxHistory.Lock()
	defer lsr.mtxHistory.Unlock()
	if _, ok := lsr.history[knownNextGLSN]; !ok {
		lsr.history[knownNextGLSN] = reports
	}
	// NOTE: history map will be small - most 2 elements
	// TODO: remove this after implementing log stream-wise report
	for nextGLSN := range lsr.history {
		if nextGLSN < knownNextGLSN {
			delete(lsr.history, nextGLSN)
		}
	}
	return knownNextGLSN, reports
}

func (lsr *logStreamReporter) Commit(nextGLSN, prevNextGLSN types.GLSN, commitResults []CommittedLogStreamStatus) {
	if !lsr.verifyCommit(prevNextGLSN) {
		return
	}
	if len(commitResults) == 0 {
		return
	}
	lsr.commitC <- lsrCommitTask{
		nextGLSN:      nextGLSN,
		prevNextGLSN:  prevNextGLSN,
		commitResults: commitResults,
	}
}

func (lsr *logStreamReporter) commit(t lsrCommitTask) {
	if !lsr.verifyCommit(t.prevNextGLSN) {
		return
	}
	for _, commitResult := range t.commitResults {
		logStreamID := commitResult.LogStreamID
		lsr.mtxExecutors.RLock()
		executor, ok := lsr.executors[logStreamID]
		lsr.mtxExecutors.RUnlock()
		if !ok {
			panic("no such executor")
		}
		// TODO: check returned value, and log it
		// TODO: run goroutine
		executor.Commit(commitResult)
	}
	lsr.knownNextGLSN.Store(t.nextGLSN)
}

func (lsr *logStreamReporter) verifyCommit(prevNextGLSN types.GLSN) bool {
	knownNextGLSN := lsr.knownNextGLSN.Load()
	return prevNextGLSN == knownNextGLSN
}
