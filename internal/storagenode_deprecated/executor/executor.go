package executor

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/executor -package executor -destination executor_mock.go . Executor

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/id"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/logio"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/replication"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/reportcommitter"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/storage"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/timestamper"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type Executor interface {
	io.Closer
	logio.ReadWriter
	reportcommitter.ReportCommitter
	SealUnsealer
	replication.Replicator
	MetadataProvider
	id.StorageNodeIDGetter
	id.LogStreamIDGetter
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

		muReasonToSeal sync.RWMutex
		reasonToSeal   error
	}

	// muState guards the state of executor from RPCs, for instance, Seal, Unseal, SyncInit, and
	// SyncReplicate. By this mutex, only one RPC handler can execute at the same time, thus,
	// the state of executor is updated mutually exclusively.
	muState sync.Mutex

	running struct {
		val bool
		mu  sync.RWMutex
	}

	decider *decidableCondition

	// srs is guarded by muState.
	// TODO: watchdog
	srs *syncReplicateState

	syncTrackers struct {
		mu  sync.Mutex
		trk *syncTracker
	}

	tsp timestamper.Timestamper

	// The primaryBackups is a slice of replicas of a log stream. It is updated by Unseal
	// and is read by many codes.
	primaryBackups []varlogpb.LogStreamReplica
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

	lse.syncTrackers.trk = newSyncTracker(lse.sync)

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
	lse.lsc = lse.restoreLogStreamContext(ri)

	lse.decider = newDecidableCondition(lse.lsc)

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
	lastCommittedGLSN := lse.lsc.localHighWatermark().GLSN
	lse.storage.RestoreStorage(lastWrittenLLSN, lastCommittedLLSN, lastCommittedGLSN)

	// The executor should start in the sealing phase.
	lse.setSealingWithReason(errors.New("initial state"))

	return lse, nil
}

func (e *executor) initLogPipeline() error {
	// replication processor
	rp, err := newReplicator(replicatorConfig{
		queueSize: e.replicateQueueSize,
		state:     e,
		metrics:   e.metrics,
		connectorOpts: []replication.ConnectorOption{
			replication.WithClientOptions(
				replication.WithMetrics(e.metrics),
				replication.WithGRPCDialOptions(
					grpc.WithReadBufferSize(e.replicationClientReadBufferSize),
					grpc.WithWriteBufferSize(e.replicationClientWriteBufferSize),
				),
			),
		},
	})
	if err != nil {
		return err
	}
	e.rp = rp

	committer, err := newCommitter(committerConfig{
		commitTaskQueueSize: e.commitTaskQueueSize,
		commitTaskBatchSize: e.commitBatchSize,
		commitQueueSize:     e.commitQueueSize,
		strg:                e.storage,
		lsc:                 e.lsc,
		decider:             e.decider,
		state:               e,
		metrics:             e.metrics,
	})
	if err != nil {
		return err
	}
	e.committer = committer

	writer, err := newWriter(writerConfig{
		queueSize:  e.writeQueueSize,
		batchSize:  e.writeBatchSize,
		strg:       e.storage,
		lsc:        e.lsc,
		committer:  e.committer,
		replicator: e.rp,
		state:      e,
		metrics:    e.metrics,
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

func (e *executor) restoreLogStreamContext(ri storage.RecoveryInfo) *logStreamContext {
	lsc := newLogStreamContext()
	commitVersion, highWatermark, uncommittedLLSNBegin := lsc.reportCommitBase()
	uncommittedLLSNEnd := lsc.uncommittedLLSNEnd.Load()
	lsc.commitProgress.mu.RLock()
	committedLLSNEnd := lsc.commitProgress.committedLLSNEnd
	lsc.commitProgress.mu.RUnlock()
	localHighWatermark := lsc.localHighWatermark()
	localLowWatermark := lsc.localLowWatermark()

	if ri.LastCommitContext.Found {
		commitVersion = ri.LastCommitContext.CC.Version
		highWatermark = ri.LastCommitContext.CC.HighWatermark
	}
	if ri.LogEntryBoundary.Found {
		lastLLSN := ri.LogEntryBoundary.Last.LLSN
		uncommittedLLSNBegin = lastLLSN + 1
		uncommittedLLSNEnd = lastLLSN + 1
		committedLLSNEnd = lastLLSN + 1

		localHighWatermark = ri.LogEntryBoundary.Last
		localLowWatermark = ri.LogEntryBoundary.First
	}

	lsc.storeReportCommitBase(commitVersion, highWatermark, uncommittedLLSNBegin)
	lsc.uncommittedLLSNEnd.Store(uncommittedLLSNEnd)
	lsc.commitProgress.mu.Lock()
	lsc.commitProgress.committedLLSNEnd = committedLLSNEnd
	lsc.commitProgress.mu.Unlock()
	lsc.setLocalHighWatermark(localHighWatermark)
	lsc.setLocalLowWatermark(localLowWatermark)
	return lsc
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
		e.primaryBackups[0].StorageNode.StorageNodeID == e.storageNodeID &&
		e.primaryBackups[0].LogStreamID == e.logStreamID
}

func (e *executor) StorageNodeID() types.StorageNodeID {
	return e.storageNodeID
}

func (e *executor) TopicID() types.TopicID {
	return e.topicID
}

func (e *executor) LogStreamID() types.LogStreamID {
	return e.logStreamID
}
