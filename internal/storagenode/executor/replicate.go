package executor

import (
	"context"

	"go.uber.org/zap"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/internal/storagenode/replication"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func (e *executor) Replicate(ctx context.Context, llsn types.LLSN, data []byte) error {
	if err := e.guard(); err != nil {
		return err
	}
	defer e.unguard()

	tb := newAppendTask()
	defer tb.release()

	tb.primary = false
	tb.replicas = nil
	tb.llsn = llsn
	tb.data = data
	tb.wg.Add(1)

	if err := e.writer.send(ctx, tb); err != nil {
		tb.wg.Done()
		tb.wg.Wait()
		return err
	}

	tb.wg.Wait()
	return nil
}

func (e *executor) SyncInit(ctx context.Context, first, last snpb.SyncPosition) (snpb.SyncPosition, error) {
	panic("not implemented")
}

func (e *executor) SyncReplicate(ctx context.Context, payload snpb.SyncPayload) error {
	if err := e.guard(); err != nil {
		return err
	}
	defer e.unguard()

	if e.stateBarrier.state.load() != executorSealing {
		return errors.New("invalid state")
	}

	e.lsc.commitProgress.mu.RLock()
	committedLLSNEnd := e.lsc.commitProgress.committedLLSNEnd
	e.lsc.commitProgress.mu.RUnlock()

	if payload.GetLogEntry().GetLLSN() < committedLLSNEnd {
		return nil
	}

	return errors.New("not yet implemented")
}

func (e *executor) Sync(ctx context.Context, replica snpb.Replica, lastGLSN types.GLSN) (*replication.SyncTaskStatus, error) {
	if err := e.guard(); err != nil {
		return nil, err
	}
	defer e.unguard()

	// TODO (jun): Delete SyncTaskStatus, but when?
	if e.stateBarrier.state.load() != executorSealed {
		return nil, errors.New("invalid state")
	}

	e.syncTracker.mu.Lock()
	defer e.syncTracker.mu.Unlock()

	if sts, ok := e.syncTracker.tracker[replica.GetStorageNodeID()]; ok {
		sts.Mu.RLock()
		defer sts.Mu.RUnlock()
		// FIXME (jun): Deleting sync history this point is not good.
		if sts.Err != nil {
			delete(e.syncTracker.tracker, replica.GetStorageNodeID())
		}
		return &replication.SyncTaskStatus{
			Replica: sts.Replica,
			State:   sts.State,
			First:   sts.First,
			Last:    sts.Last,
			Current: sts.Current,
		}, nil
	}

	firstGLSN := e.lsc.localGLSN.localLowWatermark.Load()
	firstLE, err := e.storage.Read(firstGLSN)
	if err != nil {
		// error
	}
	firstLLSN := firstLE.LLSN

	lastLE, err := e.storage.Read(lastGLSN)
	if err != nil {
		// error
	}
	lastLLSN := lastLE.LLSN

	first := snpb.SyncPosition{LLSN: firstLLSN, GLSN: firstGLSN}
	last := snpb.SyncPosition{LLSN: lastLLSN, GLSN: lastGLSN}
	current := snpb.SyncPosition{LLSN: types.InvalidLLSN, GLSN: types.InvalidGLSN}

	sts := &replication.SyncTaskStatus{
		Replica: replica,
		State:   snpb.SyncStateInProgress,
		First:   first,
		Last:    last,
		Current: current,
	}
	e.syncTracker.tracker[replica.GetStorageNodeID()] = sts

	// TODO: copy
	return sts, err
}

func (e *executor) syncer(ctx context.Context, sts *replication.SyncTaskStatus) func(context.Context) {
	first, last, current := sts.First, sts.Last, sts.Current
	return func(ctx context.Context) {
		defer sts.Cancel()

		var err error
		numLogs := types.LLSN(0)

		subEnv, err := e.Subscribe(ctx, first.GLSN, last.GLSN+1)
		if err != nil {
			// handle it
			goto errOut
		}
		defer subEnv.Stop()

		for result := range subEnv.ScanResultC() {
			if result.LogEntry.LLSN != first.LLSN+numLogs {
				err = errors.Errorf("logstream: unexpected LLSN (expected = %v, actual = %v)", first.LLSN+numLogs, result.LogEntry.LLSN)
				break
			}
			numLogs++

			payload := snpb.SyncPayload{
				LogEntry: &varlogpb.LogEntry{
					LLSN: result.LogEntry.LLSN,
					GLSN: result.LogEntry.GLSN,
					Data: result.LogEntry.Data,
				},
			}
			if err = e.SyncReplicate(ctx, payload); err != nil {
				break
			}

			// update status
			sts.Mu.Lock()
			sts.Current = current
			sts.Mu.Unlock()
		}

	errOut:
		sts.Mu.Lock()
		if err == nil {
			e.logger.Debug("syncer completed", zap.Any("first", first), zap.Any("last", last), zap.Any("current", current))
			sts.State = snpb.SyncStateComplete
		} else {
			e.logger.Error("syncer failed", zap.Error(err), zap.Any("first", first), zap.Any("last", last), zap.Any("current", current))
			sts.State = snpb.SyncStateError
		}
		sts.Err = err
		sts.Mu.Unlock()
	}
}
