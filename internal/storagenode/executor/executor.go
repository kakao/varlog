package executor

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/kakao/varlog/internal/storagenode/logio"
	"github.com/kakao/varlog/internal/storagenode/replication"
	"github.com/kakao/varlog/internal/storagenode/reportcommitter"
	"github.com/kakao/varlog/internal/storagenode/storage"
	"github.com/kakao/varlog/internal/storagenode/timestamper"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

type Executor interface {
	io.Closer
	logio.ReadWriter
	reportcommitter.ReportCommitter
	SealUnsealer
	replication.Replicator
	MetadataProvider
}

type executor struct {
	config

	lsc *logStreamContext

	writer writer

	rp replicator

	committer committer

	deferredTrim struct {
		safetyGap types.GLSN
		glsn      types.GLSN
		mu        sync.RWMutex
	}

	stateBarrier struct {
		state atomicExecutorState
		lock  sync.RWMutex
	}

	muSeal sync.Mutex

	running struct {
		val bool
		mu  sync.RWMutex
	}

	decider *decidableCondition

	syncTracker struct {
		tracker map[types.StorageNodeID]*replication.SyncTaskStatus
		mu      sync.Mutex
	}

	tsp timestamper.Timestamper

	// The primaryBackups is a slice of replicas of a log stream. It is updated by Unseal
	// and is read by many codes.
	primaryBackups []snpb.Replica
}

var _ Executor = (*executor)(nil)

func New(opts ...Option) (*executor, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	lse := &executor{
		config: *cfg,
		tsp:    timestamper.New(),
	}

	// lse.replicas.Store([]snpb.Replica{})

	lse.running.val = true
	// NOTE: To push commitWaitTask into the commitWaitQ, the state of this executor is
	// executorMutable temporarily.
	lse.stateBarrier.state.store(executorMutable)

	// read RecoveryInfo from storage
	ri, err := lse.storage.ReadRecoveryInfo()
	if err != nil {
		return nil, err
	}

	// restore LogStreamContext
	// NOTE: LogStreamContext should be restored before initLogPipeline is called.
	lsc, err := lse.restoreLogStreamContext(ri)
	if err != nil {
		return nil, err
	}
	lse.lsc = lsc
	lse.decider = newDecidableCondition(lsc)

	// init log pipeline
	if err := lse.initLogPipeline(); err != nil {
		return nil, err
	}

	// regenerate commitWaitTasks
	if err := lse.regenerateCommitWaitTasks(ri); err != nil {
		return nil, err
	}

	// NOTE: Storage doesn't need to be stateful, but the current implementation has information
	// about the progress of LLSN and GLSN. If the implementations of executor and storage are
	// stable, those states in the storage can be removed.
	lastWrittenLLSN := lse.lsc.uncommittedLLSNEnd.Load() - 1
	lastCommittedLLSN := lse.lsc.commitProgress.committedLLSNEnd - 1
	lastCommittedGLSN := lse.lsc.localGLSN.localHighWatermark.Load()
	lse.storage.RestoreStorage(lastWrittenLLSN, lastCommittedLLSN, lastCommittedGLSN)

	// The executor should start in the sealing phase.
	lse.stateBarrier.state.store(executorSealing)

	return lse, nil
}

func (e *executor) initLogPipeline() error {
	// replication processor
	rp, err := newReplicator(replicatorConfig{
		queueSize: e.replicateQueueSize,
		state:     e,
	})
	if err != nil {
		return err
	}
	e.rp = rp

	committer, err := newCommitter(committerConfig{
		commitTaskQueueSize: e.commitTaskQueueSize,
		commitTaskBatchSize: e.committerBatchSize,
		commitQueueSize:     e.commitQueueSize,
		strg:                e.storage,
		lsc:                 e.lsc,
		decider:             e.decider,
		state:               e,
	})
	if err != nil {
		return err
	}
	e.committer = committer

	writer, err := newWriter(writerConfig{
		queueSize:  e.writeQueueSize,
		batchSize:  e.writerBatchSize,
		strg:       e.storage,
		lsc:        e.lsc,
		committer:  e.committer,
		replicator: e.rp,
		state:      e,
	})
	if err != nil {
		rp.stop()
		committer.stop()
		return err
	}
	e.writer = writer

	return nil
}

func (e *executor) Close() (err error) {
	// stop writer, replicator, committer
	e.writer.stop()
	e.rp.stop()
	e.committer.stop()

	e.guardForClose()

	// last cleanup
	e.decider.destroy()
	return multierr.Append(err, e.storage.Close())
}

func (e *executor) restoreLogStreamContext(ri storage.RecoveryInfo) (*logStreamContext, error) {
	lsc := newLogStreamContext()
	globalHighWatermark, uncommittedLLSNBegin := lsc.reportCommitBase()
	uncommittedLLSNEnd := lsc.uncommittedLLSNEnd.Load()
	lsc.commitProgress.mu.RLock()
	committedLLSNEnd := lsc.commitProgress.committedLLSNEnd
	lsc.commitProgress.mu.RUnlock()
	localHighWatermark := lsc.localGLSN.localHighWatermark.Load()
	localLowWatermark := lsc.localGLSN.localLowWatermark.Load()

	if ri.LastCommitContext.Found {
		globalHighWatermark = ri.LastCommitContext.CC.HighWatermark
	}
	if ri.LogEntryBoundary.Found {
		lastLLSN := ri.LogEntryBoundary.Last.LLSN
		uncommittedLLSNBegin = lastLLSN + 1
		uncommittedLLSNEnd = lastLLSN + 1
		committedLLSNEnd = lastLLSN + 1

		localHighWatermark = ri.LogEntryBoundary.Last.GLSN
		localLowWatermark = ri.LogEntryBoundary.First.GLSN
	}

	lsc.storeReportCommitBase(globalHighWatermark, uncommittedLLSNBegin)
	lsc.uncommittedLLSNEnd.Store(uncommittedLLSNEnd)
	lsc.commitProgress.mu.Lock()
	lsc.commitProgress.committedLLSNEnd = committedLLSNEnd
	lsc.commitProgress.mu.Unlock()
	lsc.localGLSN.localHighWatermark.Store(localHighWatermark)
	lsc.localGLSN.localLowWatermark.Store(localLowWatermark)
	return lsc, nil
}

func (e *executor) regenerateCommitWaitTasks(ri storage.RecoveryInfo) error {
	if ri.UncommittedLogEntryBoundary.First.Invalid() {
		return nil
	}

	firstLLSN := ri.UncommittedLogEntryBoundary.First
	lastLLSN := ri.UncommittedLogEntryBoundary.Last
	for llsn := firstLLSN; llsn <= lastLLSN; llsn++ {
		cwt := newCommitWaitTask(llsn, nil)
		if err := e.committer.sendCommitWaitTask(context.Background(), cwt); err != nil {
			return err
		}
	}
	e.lsc.uncommittedLLSNEnd.Store(lastLLSN + 1)
	return nil
}

func (e *executor) guard() error {
	e.running.mu.RLock()
	if !e.running.val {
		e.running.mu.RUnlock()
		return errors.WithStack(verrors.ErrClosed)
	}
	return nil
}

func (e *executor) unguard() {
	e.running.mu.RUnlock()
}

func (e *executor) guardForClose() {
	e.running.mu.Lock()
	e.running.val = false
	e.running.mu.Unlock()
}

func (e *executor) withRecover(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if maybeErr, ok := r.(error); ok {
				err = maybeErr
			} else {
				err = errors.Errorf("%v", r)
			}
			return
		}
	}()
	err = f()
	return err
}

// isPrimary returns true if the executor is behind of primary replica. Note that isPrimary should
// be called within the scope of the mutex for stateBarrier.
func (e *executor) isPrimay() bool {
	// NOTE: A new log stream replica that has not received Unseal request is not primary
	// replica.
	return len(e.primaryBackups) > 0 &&
		e.primaryBackups[0].StorageNodeID == e.storageNodeID &&
		e.primaryBackups[0].LogStreamID == e.logStreamID
}
