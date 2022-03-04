package logstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storage"
	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/mathutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type syncTracker struct {
	syncRange struct {
		first varlogpb.LogEntryMeta
		last  varlogpb.LogEntryMeta
	}

	mu     sync.Mutex
	cursor varlogpb.LogEntryMeta
}

func newSyncTracker(first varlogpb.LogEntryMeta, last varlogpb.LogEntryMeta) *syncTracker {
	st := &syncTracker{}
	st.syncRange.first = first
	st.syncRange.last = last
	return st
}

func (st *syncTracker) toSyncStatus() *snpb.SyncStatus {
	st.mu.Lock()
	defer st.mu.Unlock()
	return &snpb.SyncStatus{
		State: snpb.SyncStateInProgress,
		First: snpb.SyncPosition{
			LLSN: st.syncRange.first.LLSN,
			GLSN: st.syncRange.first.GLSN,
		},
		Last: snpb.SyncPosition{
			LLSN: st.syncRange.last.LLSN,
			GLSN: st.syncRange.last.GLSN,
		},
		Current: snpb.SyncPosition{
			LLSN: st.cursor.LLSN,
			GLSN: st.cursor.GLSN,
		},
	}
}

func (st *syncTracker) setCursor(cursor varlogpb.LogEntryMeta) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.cursor = cursor
}

func (st *syncTracker) end() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.syncRange.last.LLSN == st.cursor.LLSN
}

func (lse *Executor) Sync(ctx context.Context, dstReplica varlogpb.Replica) (*snpb.SyncStatus, error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	lse.muAdmin.Lock()
	defer lse.muAdmin.Unlock()

	if state := lse.esm.load(); state != executorStateSealed {
		if state == executorStateClosed {
			return nil, fmt.Errorf("log stream: sync: %w", verrors.ErrClosed)
		}
		return nil, fmt.Errorf("log stream: sync: invalid state %d: %w", state, verrors.ErrInvalid)
	}

	if st, ok := lse.sts[dstReplica.StorageNodeID]; ok {
		return st.toSyncStatus(), nil
	}

	rpcConn, err := rpc.NewConn(ctx, dstReplica.Address)
	if err != nil {
		return nil, err
	}

	sc := newSyncClient(syncClientConfig{
		dstReplica: dstReplica,
		rpcConn:    rpcConn,
		lse:        lse,
		logger:     lse.logger.Named("sync client").With(zap.String("dst", dstReplica.String())),
	})

	localLWM, localHWM := lse.lsc.localLowWatermark(), lse.lsc.localHighWatermark()
	syncRange, err := sc.syncInit(ctx, snpb.SyncRange{
		FirstLLSN: localLWM.LLSN,
		LastLLSN:  localHWM.LLSN,
	})
	if err != nil {
		return nil, err
	}
	if syncRange.FirstLLSN.Invalid() && syncRange.LastLLSN.Invalid() {
		return &snpb.SyncStatus{
			State: snpb.SyncStateComplete,
			First: snpb.SyncPosition{
				LLSN: localLWM.LLSN,
				GLSN: localLWM.GLSN,
			},
			Last: snpb.SyncPosition{
				LLSN: localHWM.LLSN,
				GLSN: localHWM.GLSN,
			},
		}, nil
	}

	first, err := lse.stg.Read(storage.AtLLSN(syncRange.FirstLLSN))
	if err != nil {
		return nil, err
	}

	// make tracker
	st := newSyncTracker(first.LogEntryMeta, localHWM)
	lse.sts[dstReplica.StorageNodeID] = st
	_, _ = lse.syncRunner.Run(func(ctx context.Context) {
		snid := sc.dstReplica.StorageNodeID
		defer func() {
			lse.muAdmin.Lock()
			delete(lse.sts, snid)
			lse.muAdmin.Unlock()
		}()
		lse.syncLoop(ctx, sc, st)
	})
	return st.toSyncStatus(), nil
}

func (lse *Executor) syncLoop(ctx context.Context, sc *syncClient, st *syncTracker) {
	var (
		err error
		cc  storage.CommitContext
		sr  *SubscribeResult
	)
	defer func() {
		if err == nil {
			lse.logger.Info("sync completed", zap.String("status", st.toSyncStatus().String()))
		} else {
			lse.logger.Error("could not sync", zap.Error(err))
		}
		_ = sc.close()
	}()

	cc, err = lse.stg.CommitContextOf(st.syncRange.first.GLSN)
	if err != nil {
		return
	}

	for ctx.Err() == nil && lse.esm.load() == executorStateSealed {
		// Configure syncReplicate timeout
		err = sc.syncReplicate(context.TODO(), snpb.SyncPayload{
			CommitContext: &varlogpb.CommitContext{
				Version:            cc.Version,
				HighWatermark:      cc.HighWatermark,
				CommittedGLSNBegin: cc.CommittedGLSNBegin,
				CommittedGLSNEnd:   cc.CommittedGLSNEnd,
				CommittedLLSNBegin: cc.CommittedLLSNBegin,
			},
		})
		if err != nil {
			return
		}

		if cc.Empty() {
			cc, err = lse.stg.NextCommitContextOf(cc)
			if err != nil {
				return
			}
			continue
		}

		// The first commit context may not have full log entries due to trim.
		beginGLSN := types.GLSN(mathutil.MaxUint64(uint64(cc.CommittedGLSNBegin), uint64(st.syncRange.first.GLSN)))
		sr, err = lse.SubscribeWithGLSN(beginGLSN, cc.CommittedGLSNEnd)
		if err != nil {
			return
		}
		for le := range sr.Result() {
			// Configure syncReplicate timeout
			err = sc.syncReplicate(context.TODO(), snpb.SyncPayload{LogEntry: &le})
			if err != nil {
				sr.Stop()
				return
			}
			st.setCursor(le.LogEntryMeta)
		}
		sr.Stop()
		err = sr.Err()
		if err != nil {
			return
		}

		if st.end() {
			return
		}

		cc, err = lse.stg.NextCommitContextOf(cc)
		if err != nil {
			return
		}
	}
}

// syncReplicateBuffer represents the progress of sync replication.
type syncReplicateBuffer struct {
	srcReplica varlogpb.Replica
	// syncRange is a range of overall sync replication, which is set for the first time.
	syncRange snpb.SyncRange

	cc           *varlogpb.CommitContext
	les          []varlogpb.LogEntry
	prevCC       *varlogpb.CommitContext
	expectedLLSN types.LLSN

	updatedAt time.Time
}

func newSyncReplicateBuffer(srcReplica varlogpb.Replica, syncRange snpb.SyncRange) (*syncReplicateBuffer, error) {
	if syncRange.Invalid() {
		return nil, fmt.Errorf("sync replicate: invalid sync range %s", syncRange.String())
	}
	return &syncReplicateBuffer{
		srcReplica:   srcReplica,
		syncRange:    syncRange,
		expectedLLSN: syncRange.FirstLLSN,
	}, nil
}

// add inserts either a commit context or a log entry.
// If the srb already has a commit context, the payload should be a log entry.
// If the srb does not have a commit context, the payload should be a commit context.
func (srb *syncReplicateBuffer) add(srcReplica varlogpb.Replica, payload snpb.SyncPayload, updatedAt time.Time) (err error) {
	if !srb.srcReplica.Equal(srcReplica) {
		return fmt.Errorf("log stream: sync replicate: invalid source replica %s", srcReplica)
	}
	defer func() {
		if err == nil {
			srb.updatedAt = updatedAt
		}
	}()
	if payload.CommitContext != nil {
		err = srb.addCommitContext(payload.CommitContext)
	} else if payload.LogEntry != nil {
		err = srb.addLogEntry(payload.LogEntry)
	} else {
		err = errors.New("log stream: sync replicate: invalid payload")
	}
	return err
}

func (srb *syncReplicateBuffer) addCommitContext(cc *varlogpb.CommitContext) error {
	if srb.cc != nil || len(srb.les) > 0 {
		return errors.New("log stream: sync replicate: previous sync replication is not completed")
	}
	if cc.CommittedGLSNBegin > cc.CommittedGLSNEnd {
		return errors.New("log stream: sync replicate: invalid commit context")
	}
	if srb.prevCC != nil && cc.CommittedLLSNBegin != srb.prevCC.CommittedLLSNBegin+types.LLSN(srb.prevCC.CommittedGLSNEnd-srb.prevCC.CommittedGLSNBegin) {
		return errors.New("log stream: sync replicate: not sequential sync replication")
	}
	srb.cc = cc
	srb.les = make([]varlogpb.LogEntry, 0, cc.CommittedGLSNEnd-cc.CommittedGLSNBegin)
	return nil
}

func (srb *syncReplicateBuffer) addLogEntry(le *varlogpb.LogEntry) error {
	if srb.cc == nil {
		return errors.New("log stream: sync replicate: no commit context")
	}
	if srb.cc.CommittedGLSNEnd <= le.GLSN {
		return fmt.Errorf("log stream: sync replicate: unexpected log entry %s, commit context %s", le.String(), srb.cc.String())
	}
	if srb.expectedLLSN != le.LLSN {
		return fmt.Errorf("log stream: sync replicate: unexpected log entry %s, expected llsn %d", le.String(), srb.expectedLLSN)

	}
	if length := len(srb.les); length > 0 {
		expectedGLSN := srb.les[length-1].GLSN + 1
		if expectedGLSN != le.GLSN {
			return fmt.Errorf("log stream: sync replicate: unexpected log entry %s, expected glsn %d", le.String(), expectedGLSN)
		}
	}
	srb.les = append(srb.les, *le)
	srb.expectedLLSN++
	return nil
}

// committable decides whether the commit context and log entries in the srb can be committed.
func (srb *syncReplicateBuffer) committable() bool {
	ccLen := uint64(srb.cc.CommittedGLSNEnd - srb.cc.CommittedGLSNBegin)
	if ccLen == 0 {
		return true
	}
	lesLen := uint64(len(srb.les))
	return srb.les[lesLen-1].GLSN == srb.cc.CommittedGLSNEnd-1
}

// end decides whether the sync replication is done.
func (srb *syncReplicateBuffer) end() bool {
	length := len(srb.les)
	return length > 0 && srb.les[length-1].LLSN == srb.syncRange.LastLLSN
}

// reset clears current commit context and log entries.
func (srb *syncReplicateBuffer) reset() {
	srb.prevCC = srb.cc
	srb.cc = nil
	srb.les = nil
}

func (lse *Executor) SyncInit(_ context.Context, srcReplica varlogpb.Replica, srcRange snpb.SyncRange) (syncRange snpb.SyncRange, err error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	lse.muAdmin.Lock()
	defer lse.muAdmin.Unlock()

	if state := lse.esm.load(); state != executorStateSealing {
		if state == executorStateClosed {
			err = fmt.Errorf("log stream: sync init: %w", verrors.ErrClosed)
			return
		}
		if state != executorStateLearning || (lse.srb != nil && time.Since(lse.srb.updatedAt) < lse.syncInitTimeout) {
			err = fmt.Errorf("log stream: sync init: invalid state %d: %w", state, verrors.ErrInvalid)
			return
		}
		// expire timed-out srb
		lse.esm.store(executorStateSealing)
		lse.srb = nil
	}

	if srcRange.Invalid() {
		err = fmt.Errorf("log stream: sync init: invalid range %s: %w", srcRange.String(), verrors.ErrInvalid)
		return
	}

	_, _, uncommittedLLSNBegin := lse.lsc.reportCommitBase()
	lastCommittedLLSN := uncommittedLLSNBegin - 1
	if lastCommittedLLSN > srcRange.LastLLSN {
		lse.logger.Panic("sync init: destination of sync has too many logs",
			zap.String("src_range", srcRange.String()),
			zap.Uint64("last_committed_llsn", uint64(lastCommittedLLSN)),
		)
	}

	syncRange = snpb.SyncRange{
		FirstLLSN: uncommittedLLSNBegin,
		LastLLSN:  srcRange.LastLLSN,
	}

	// NOTE: When the replica has all log entries, it returns its range of logs and non-error results.
	// In this case, this replica remains executorStateSealing.
	// Breaking change: previously it returns ErrExist when the replica has all log entries to replicate.
	if lastCommittedLLSN == syncRange.LastLLSN {
		return snpb.SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.InvalidLLSN}, nil
		//err = fmt.Errorf("log stream: sync init: already enough logs: %w", verrors.ErrExist)
		//return
	}

	// FIXME(jun): It should be necessary to have a mechanism to expire long-time sync init state.
	if !lse.esm.compareAndSwap(executorStateSealing, executorStateLearning) {
		err = fmt.Errorf("log stream: sync init: invalid state %d: %w", lse.esm.load(), verrors.ErrInvalid)
		return
	}

	// learning
	lse.resetInternalState(lastCommittedLLSN, !lse.isPrimary())
	srb, err := newSyncReplicateBuffer(srcReplica, syncRange)
	if err != nil {
		lse.esm.store(executorStateSealing)
		return syncRange, err
	}
	lse.srb = srb
	return syncRange, nil
}

func (lse *Executor) SyncReplicate(_ context.Context, srcReplica varlogpb.Replica, payload snpb.SyncPayload) (err error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	lse.muAdmin.Lock()
	defer lse.muAdmin.Unlock()

	if state := lse.esm.load(); state != executorStateLearning {
		if state == executorStateClosed {
			return fmt.Errorf("log stream: sync replicate: %w", verrors.ErrClosed)
		}
		return fmt.Errorf("log stream: sync replicate: invalid state %d: %w", state, verrors.ErrInvalid)
	}

	defer func() {
		if err != nil {
			lse.srb = nil
			lse.esm.store(executorStateSealing)
		}
	}()

	err = lse.srb.add(srcReplica, payload, time.Now())
	if err != nil {
		return err
	}

	ccLen := int(lse.srb.cc.CommittedGLSNEnd - lse.srb.cc.CommittedGLSNBegin)
	leCnt := len(lse.srb.les)
	if ccLen < leCnt {
		lse.logger.Panic("sync replicate: too many log entries", zap.Any("srb", lse.srb))
	}
	if ccLen > len(lse.srb.les) {
		return nil
	}

	wb := lse.stg.NewWriteBatch()
	defer func() {
		_ = wb.Close()
	}()
	for _, le := range lse.srb.les {
		err = wb.Set(le.LLSN, le.Data)
		if err != nil {
			return err
		}
	}
	err = wb.Apply()
	if err != nil {
		return err
	}
	lse.lsc.uncommittedLLSNEnd.Add(uint64(ccLen))

	err = lse.cm.commitInternal(storage.CommitContext{
		Version:            lse.srb.cc.Version,
		HighWatermark:      lse.srb.cc.HighWatermark,
		CommittedGLSNBegin: lse.srb.cc.CommittedGLSNBegin,
		CommittedGLSNEnd:   lse.srb.cc.CommittedGLSNEnd,
		CommittedLLSNBegin: lse.srb.cc.CommittedLLSNBegin,
	}, false)
	if err != nil {
		return err
	}

	end := lse.srb.end()
	if !end {
		lse.srb.reset()
		return nil
	}

	lse.esm.store(executorStateSealing)
	lse.srb = nil
	return nil
}
