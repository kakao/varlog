package executor

import (
	"io"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/logio"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/replication"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/reportcommitter"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/timestamper"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

type Executor interface {
	io.Closer
	logio.ReadWriter
	reportcommitter.ReportCommitter
	SealUnsealer
	replication.Replicator
	MetadataProvider

	Path() string
}

type executor struct {
	config

	lsc *logStreamContext

	writer writer

	rp               replicator
	replicaConnector replication.Connector

	committer   committer
	commitTaskQ commitTaskQueue

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

	lse.running.val = true

	lsc, err := lse.restoreLogStreamContext()
	if err != nil {
		return nil, err
	}
	lse.lsc = lsc
	lse.decider = newDecidableCondition(lsc)

	replicaConnector, err := replication.NewConnector()
	if err != nil {
		return nil, err
	}
	lse.replicaConnector = replicaConnector

	// resettable fields
	if err := lse.init(); err != nil {
		return nil, multierr.Append(err, replicaConnector.Close())
	}

	return lse, nil
}

func (e *executor) init() error {
	var (
		err       error
		rp        *replicatorImpl
		writer    *writerImpl
		committer *committerImpl
	)

	// replication processor
	rp, err = newReplicator(replicatorConfig{
		queueSize: e.replicateQueueSize,
		connector: e.replicaConnector,
		state:     e,
	})
	if err != nil {
		return err
	}
	e.rp = rp

	committer, err = newCommitter(committerConfig{
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

	writer, err = newWriter(writerConfig{
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

	// close replica connector
	err = multierr.Append(err, e.replicaConnector.Close())

	e.guardForClose()

	// last cleanup
	e.decider.destroy()
	return multierr.Append(err, e.storage.Close())
}

func (e *executor) restoreLogStreamContext() (*logStreamContext, error) {
	ri, err := e.storage.ReadRecoveryInfo()
	if err != nil {
		return nil, err
	}

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
