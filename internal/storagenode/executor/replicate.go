package executor

import (
	"context"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/logio"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/replication"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func (e *executor) Replicate(ctx context.Context, llsn types.LLSN, data []byte) error {
	if err := e.guard(); err != nil {
		return err
	}
	defer e.unguard()

	twg := newTaskWaitGroup()
	wt := newBackupWriteTask(twg, data, llsn)
	defer func() {
		wt.release()
		twg.release()
	}()

	wt.validate = func() error {
		if e.isPrimay() {
			return errors.Wrapf(verrors.ErrInvalid, "primary replica")
		}
		return nil
	}

	if err := e.writer.send(ctx, wt); err != nil {
		twg.wg.Done()
		return err
	}

	twg.wg.Wait()
	return twg.err
}

func (e *executor) SyncInit(ctx context.Context, srcRange snpb.SyncRange) (syncRange snpb.SyncRange, err error) {
	syncRange = snpb.InvalidSyncRange()

	if srcRange.Invalid() {
		return syncRange, errors.WithStack(verrors.ErrInvalid)
	}
	if err := e.guard(); err != nil {
		return syncRange, err
	}
	defer e.unguard()

	e.muState.Lock()
	defer e.muState.Unlock()

	// TODO: check range of sync
	// if the executor already has last position committed, this RPC should be rejected.
	_, _, uncommittedLLSNBegin := e.lsc.reportCommitBase()
	lastCommittedLLSN := uncommittedLLSNBegin - 1
	if lastCommittedLLSN > srcRange.LastLLSN {
		panic("oops")
	} else if lastCommittedLLSN == srcRange.LastLLSN {
		return syncRange, errors.WithStack(verrors.ErrExist)
	}

	if !e.setLearning() {
		return syncRange, errors.WithStack(verrors.ErrInvalid)
	}

	// now LEARNING
	defer func() {
		// FIXME: extract to method
		// When the error is occurred, state should be reverted to SEALING.
		if err != nil {
			e.stateBarrier.lock.Lock()
			defer e.stateBarrier.lock.Unlock()
			e.stateBarrier.state.store(executorSealing)
		}
	}()

	err = e.resetInternalState(ctx, uncommittedLLSNBegin-1, e.lsc.localGLSN.localHighWatermark.Load(), !e.isPrimay())
	if err != nil {
		return syncRange, err
	}

	syncRange = snpb.SyncRange{
		FirstLLSN: uncommittedLLSNBegin,
		LastLLSN:  srcRange.LastLLSN,
	}

	e.srs = newSyncReplicateState(syncRange)

	return syncRange, nil

	// If this executor is primary replica,
	// - wait for writer to be empty
	// - wait for replicator to be empty (P->B)
	// - wait for committer's commitTasks to be empty (MR's Commit RPCs)
	// - do not drop commitWaitTasks (C's Append RPCs)
	// - delete uncommitted logs in storage
	// - restore storage status
	//
	// If this executor is backup replica,
	// - wait for writer to be empty
	// - wait for committer's commitTasks to be empty (MR's Commit RPCs)
	// - drop commitWaitTasks (P->B)
	// - delete uncommitted logs in storage
	// - restore storage status
}

func (e *executor) SyncReplicate(ctx context.Context, payload snpb.SyncPayload) error {
	if err := e.guard(); err != nil {
		return err
	}
	defer e.unguard()

	e.muState.Lock()
	defer e.muState.Unlock()

	if e.stateBarrier.state.load() != executorLearning {
		return errors.New("invalid executor state")
	}

	// The state of executor is now executorSealing. If SyncReplicate fails, the state of
	// executor becomes executorSealing.
	var err error
	defer func() {
		if err != nil {
			if e.srs != nil {
				e.srs.resetCommitContext()
				e.srs = nil
			}
			e.stateBarrier.lock.Lock()
			defer e.stateBarrier.lock.Unlock()
			e.stateBarrier.state.store(executorSealing)
		}
	}()

	err = e.srs.putSyncReplicatePayload(payload)
	if err != nil {
		return err
	}

	if uint64(e.srs.cc.CommittedGLSNEnd-e.srs.cc.CommittedGLSNBegin) == uint64(len(e.srs.ents)) {
		numCommits := uint64(len(e.srs.ents))

		// TODO: consider that cnt is zero
		wb := e.storage.NewWriteBatch()
		defer func() {
			_ = wb.Close()
		}()
		for _, logEntry := range e.srs.ents {
			wb.Put(logEntry.GetLLSN(), logEntry.GetData())
		}
		err = wb.Apply()
		if err != nil {
			return err
		}
		e.lsc.uncommittedLLSNEnd.Add(numCommits)

		_, err = e.committer.commitDirectly(storage.CommitContext{
			Version:            e.srs.cc.Version,
			HighWatermark:      e.srs.cc.HighWatermark,
			CommittedGLSNBegin: e.srs.cc.CommittedGLSNBegin,
			CommittedGLSNEnd:   e.srs.cc.CommittedGLSNEnd,
			CommittedLLSNBegin: e.srs.cc.CommittedLLSNBegin,
		}, false)
		if err != nil {
			return err
		}

		e.srs.resetCommitContext()

		// end of sync
		if e.srs.endOfSync() {
			// learning -> sealing
			e.stateBarrier.lock.Lock()
			defer e.stateBarrier.lock.Unlock()
			e.stateBarrier.state.store(executorSealing)

			// invalidate syncReplicateState
			e.srs = nil
		}
	}

	return nil
}

// Sync triggers syncrhonization work.
// TODO: Add unit tests for various situations.
// NOTE:
// - What if it has no entries to sync because they are trimmed?
func (e *executor) Sync(ctx context.Context, replica varlogpb.Replica) (*snpb.SyncStatus, error) {
	if err := e.guard(); err != nil {
		return nil, err
	}
	defer e.unguard()

	if e.stateBarrier.state.load() != executorSealed {
		return nil, errors.New("invalid state")
	}

	e.syncTrackers.mu.Lock()
	defer e.syncTrackers.mu.Unlock()

	if state, ok := e.syncTrackers.trk.get(replica.StorageNode.StorageNodeID); ok {
		return state.ToSyncStatus(), nil
	}

	firstGLSN := e.lsc.localGLSN.localLowWatermark.Load()
	firstLE, err := e.storage.Read(firstGLSN)
	if err != nil {
		return nil, err
	}

	lastLE, err := e.storage.Read(e.lsc.localGLSN.localHighWatermark.Load())
	if err != nil {
		return nil, err
	}

	span := snpb.SyncRange{FirstLLSN: firstLE.LLSN, LastLLSN: lastLE.LLSN}

	client, err := e.rp.clientOf(ctx, replica)
	if err != nil {
		return nil, err
	}
	span, err = client.SyncInit(ctx, span)
	if err != nil {
		if errors.Is(err, verrors.ErrExist) {
			return &snpb.SyncStatus{
				State: snpb.SyncStateComplete,
			}, nil
		}
		return nil, err
	}

	firstLE, err = e.storage.ReadAt(span.FirstLLSN)
	if err != nil {
		return nil, err
	}

	syncCtx, syncCancel := context.WithCancel(context.Background())
	state := newSyncState(syncCancel, replica, firstLE, lastLE)
	e.syncTrackers.trk.run(syncCtx, state, &e.syncTrackers.mu)
	return state.ToSyncStatus(), err
}

func (e *executor) sync(ctx context.Context, state *syncState) (err error) {
	var (
		cc     storage.CommitContext
		client replication.Client
	)

	defer func() {
		if err != nil {
			state.mu.Lock()
			state.err = err
			state.mu.Unlock()
		}
	}()

	cc, err = e.storage.CommitContextOf(state.first.GLSN)
	if err != nil {
		return err
	}

	client, err = e.rp.clientOf(ctx, state.dst)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.Close()
	}()

	for {
		// send cc
		err = client.SyncReplicate(ctx, state.dst, snpb.SyncPayload{
			CommitContext: &varlogpb.CommitContext{
				Version:            cc.Version,
				HighWatermark:      cc.HighWatermark,
				CommittedGLSNBegin: cc.CommittedGLSNBegin,
				CommittedGLSNEnd:   cc.CommittedGLSNEnd,
				CommittedLLSNBegin: cc.CommittedLLSNBegin,
			},
		})
		if err != nil {
			return err
		}

		if uint64(cc.CommittedGLSNEnd-cc.CommittedGLSNBegin) > uint64(0) {
			// scan log entries & send them
			var subEnv logio.SubscribeEnv
			subEnv, err = e.Subscribe(ctx, cc.CommittedGLSNBegin, cc.CommittedGLSNEnd)
			if err != nil {
				return err
			}
			for result := range subEnv.ScanResultC() {
				payload := snpb.SyncPayload{
					LogEntry: &varlogpb.LogEntry{
						LLSN: result.LogEntry.LLSN,
						GLSN: result.LogEntry.GLSN,
						Data: result.LogEntry.Data,
					},
				}
				err = client.SyncReplicate(ctx, state.dst, payload)
				if err != nil {
					return err
				}

				// update status
				state.mu.Lock()
				state.curr = result.LogEntry
				state.mu.Unlock()
			}
			subEnv.Stop()
		}

		// check if sync completes
		if cc.CommittedGLSNEnd-1 == state.last.GLSN {
			return nil
		}

		// next cc
		cc, err = e.storage.NextCommitContextOf(cc)
		if err != nil {
			return err
		}
	}
}
