package logstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type syncTracker struct {
	syncRange struct {
		first varlogpb.LogSequenceNumber
		last  varlogpb.LogSequenceNumber
	}

	mu     sync.Mutex
	cursor varlogpb.LogEntryMeta
}

func newSyncTracker(first varlogpb.LogSequenceNumber, last varlogpb.LogSequenceNumber) *syncTracker {
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

func (lse *Executor) Sync(ctx context.Context, dstReplica varlogpb.LogStreamReplica) (*snpb.SyncStatus, error) {
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

	defer func() {
		if err != nil {
			err = multierr.Append(err, sc.close())
		}
	}()

	localLWM, localHWM, _ := lse.lsc.localWatermarks()
	syncRange, err := sc.syncInit(ctx, snpb.SyncRange{
		FirstLLSN: localLWM.LLSN,
		LastLLSN:  localHWM.LLSN,
	})
	if err != nil {
		return nil, err
	}
	if syncRange.FirstLLSN.Invalid() && syncRange.LastLLSN.Invalid() {
		// NOTE: The sync client should be closed to avoid leaks of the
		// gRPC connection and goroutine.
		_ = sc.close()
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

	// If the FirstLLSN of the sync range is greater than the LastLLSN of
	// it, the destination has all log entries but commit context.
	var first varlogpb.LogEntry
	if syncRange.FirstLLSN <= syncRange.LastLLSN {
		first, err = lse.stg.Read(storage.AtLLSN(syncRange.FirstLLSN))
		if err != nil {
			return nil, err
		}
	}

	// make tracker
	st := newSyncTracker(varlogpb.LogSequenceNumber{
		LLSN: first.LogEntryMeta.LLSN,
		GLSN: first.LogEntryMeta.GLSN,
	}, localHWM)
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
	ss := st.toSyncStatus()
	ss.State = snpb.SyncStateStart
	return ss, nil
}

func (lse *Executor) syncLoop(_ context.Context, sc *syncClient, st *syncTracker) {
	var (
		err    error
		stream snpb.Replicator_SyncReplicateStreamClient
		cc     storage.CommitContext
		sr     *SubscribeResult
	)

	defer func() {
		if stream != nil {
			_, errStream := stream.CloseAndRecv()
			err = multierr.Append(err, errStream)
		}

		if err == nil {
			lse.logger.Info("sync completed", zap.String("status", st.toSyncStatus().String()))
		} else {
			lse.logger.Error("could not sync", zap.Error(err))
		}
		_ = sc.close()
	}()

	stream, err = sc.rpcClient.SyncReplicateStream(context.Background())
	if err != nil {
		return
	}

	req := &snpb.SyncReplicateRequest{
		ClusterID:   sc.lse.cid,
		Source:      sc.srcReplica,
		Destination: sc.dstReplica,
	}
	// NOTE: When the destination has all log entries but a commit context,
	// the syncRange.first is invalid because Sync does not look at the
	// syncRange.first.
	if !st.syncRange.first.Invalid() && st.syncRange.first.GLSN <= st.syncRange.last.GLSN {
		sr, err = lse.SubscribeWithGLSN(st.syncRange.first.GLSN, st.syncRange.last.GLSN+1)
		if err != nil {
			err = fmt.Errorf("scan: %w", err)
			return
		}
		for le := range sr.Result() {
			req.Payload.LogEntry = &le
			// TODO: Configure syncReplicate timeout
			err = stream.SendMsg(req)
			if err != nil {
				err = fmt.Errorf("sync replicate: log entry %+v: %w", le.LogEntryMeta, err)
				sr.Stop()
				return
			}
			st.setCursor(le.LogEntryMeta)
		}
		sr.Stop()
		err = sr.Err()
		if err != nil {
			err = fmt.Errorf("scan: %w", err)
			return
		}
	}

	cc, err = lse.stg.ReadCommitContext()
	if err != nil {
		err = fmt.Errorf("commit context: %w", err)
		return
	}
	if cc.CommittedLLSNBegin+types.LLSN(cc.CommittedGLSNEnd-cc.CommittedGLSNBegin)-1 != st.syncRange.last.LLSN {
		err = fmt.Errorf("commit context: invalid LLSN: %+v", cc)
	}
	if cc.CommittedGLSNEnd-1 != st.syncRange.last.GLSN {
		err = fmt.Errorf("commit context: invalid GLSN: %+v", cc)
	}
	if err != nil {
		return
	}
	req.Payload.LogEntry = nil
	req.Payload.CommitContext = &varlogpb.CommitContext{
		Version:            cc.Version,
		HighWatermark:      cc.HighWatermark,
		CommittedGLSNBegin: cc.CommittedGLSNBegin,
		CommittedGLSNEnd:   cc.CommittedGLSNEnd,
		CommittedLLSNBegin: cc.CommittedLLSNBegin,
	}
	err = stream.SendMsg(req)
}

func (lse *Executor) SyncInit(_ context.Context, srcReplica varlogpb.LogStreamReplica, srcRange snpb.SyncRange) (syncRange snpb.SyncRange, err error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	lse.muAdmin.Lock()
	defer lse.muAdmin.Unlock()

	if state := lse.esm.load(); state != executorStateSealing {
		if state == executorStateClosed {
			err = fmt.Errorf("log stream: sync init: %w", verrors.ErrClosed)
			return
		}
		if state != executorStateLearning || (time.Since(lse.dstSyncInfo.lastSyncTime) < lse.syncTimeout) {
			err = fmt.Errorf("log stream: sync init: invalid state %d: %w", state, verrors.ErrInvalid)
			return
		}
		// syncTimeout is expired.
		lse.esm.store(executorStateSealing)
	}

	if srcRange.Invalid() {
		err = fmt.Errorf("log stream: sync init: invalid range %s: %w", srcRange.String(), verrors.ErrInvalid)
		return
	}

	_, _, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
	uncommittedLLSNBegin, uncommittedGLSNBegin := uncommittedBegin.LLSN, uncommittedBegin.GLSN
	lastCommittedLLSN := uncommittedLLSNBegin - 1
	if lastCommittedLLSN > srcRange.LastLLSN {
		lse.logger.Panic("sync init: destination of sync has too many logs",
			zap.String("src_range", srcRange.String()),
			zap.Uint64("last_committed_llsn", uint64(lastCommittedLLSN)),
		)
	}

	// NOTE: When the replica has all log entries, it returns its range of logs and non-error results.
	// In this case, this replica remains executorStateSealing.
	// Breaking change: previously it returns ErrExist when the replica has all log entries to replicate.
	if lastCommittedLLSN == srcRange.LastLLSN && !invalid {
		return snpb.SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.InvalidLLSN}, nil
	}

	// The log stream replica will not send a report to the metadata
	// repository after changing its state to learning.
	// FIXME(jun): It should be necessary to have a mechanism to expire long-time sync init state.
	if !lse.esm.compareAndSwap(executorStateSealing, executorStateLearning) {
		err = fmt.Errorf("log stream: sync init: invalid state %d: %w", lse.esm.load(), verrors.ErrInvalid)
		return
	}

	trimGLSN := types.InvalidGLSN
	lwm, _, _ := lse.lsc.localWatermarks()

	if lwm.LLSN < srcRange.FirstLLSN && srcRange.FirstLLSN <= lastCommittedLLSN {
		// The source replica has already trimmed some prefix log
		// entries; thus, the destination replica should remove prefix
		// log entries to be the same as the source.

		// TODO: There are two things to do to avoid finding log
		// entries here:
		// - The source replica should propose a range of
		// synchronization denoted by GLSN and LLSN.
		// - Methods related to trim in the storage and log stream
		// should accept exclusive boundaries rather than inclusive.
		var entry varlogpb.LogEntry
		entry, err = lse.stg.Read(storage.AtLLSN(srcRange.FirstLLSN - 1))
		if err != nil {
			err = fmt.Errorf("log stream: sync init: cannot find trim position: %w", err)
			lse.esm.store(executorStateSealing)
			return
		}
		trimGLSN = entry.GLSN

		entry, err = lse.stg.Read(storage.AtLLSN(srcRange.FirstLLSN))
		if err != nil {
			err = fmt.Errorf("log stream: sync init: cannot find new lwm after trim: %w", err)
			lse.esm.store(executorStateSealing)
			return
		}

		// The local low watermark of the destination replica after
		// trimming the prefix log entries should be the same as the
		// first entry of the sync range.
		lwm = varlogpb.LogSequenceNumber{
			LLSN: entry.LLSN,
			GLSN: entry.GLSN,
		}
	} else if srcRange.FirstLLSN < lwm.LLSN || lastCommittedLLSN < srcRange.FirstLLSN {
		// The destination replica that has been trimmed prefix log
		// entries as opposed to the source replica is unusual. In this
		// case, the destination replica deletes all log entries to fix
		// it. Similarly, if the source replica has already trimmed log
		// entries that the destination has, the destination should
		// delete all log entries.
		trimGLSN = types.MaxGLSN
		lastCommittedLLSN = srcRange.FirstLLSN - 1

		// Since the destination replica will trim all log entries, the
		// local low and high watermarks will be invalid.
		// The destination replica should invalidate its local high
		// watermark by setting the GLSN of uncommittedBegin to
		// invalid.
		uncommittedGLSNBegin = types.InvalidGLSN
		// Setting an invalid log sequence number to the local low
		// watermark is necessary to invalidate it.
		lwm = varlogpb.LogSequenceNumber{}
	}

	syncRange = snpb.SyncRange{
		FirstLLSN: lastCommittedLLSN + 1,
		LastLLSN:  srcRange.LastLLSN,
	}

	if !trimGLSN.Invalid() {
		err = lse.stg.Trim(trimGLSN)
		if err != nil && !errors.Is(err, storage.ErrNoLogEntry) {
			err = fmt.Errorf("log stream: sync init: remove trimmed log entries: %w", err)
			lse.esm.store(executorStateSealing)
			return
		}

		lse.lsc.setLocalLowWatermark(lwm)
	}

	// NOTE: Invalid reportCommitBase makes the report of the log
	// stream replica meaningless.
	// The LLSN of uncommittedBegin indicates a sequence number of
	// the following log entry copied from a source replica.
	// Invalid GLSN of uncommittedBegin makes the local high
	// watermark of the replica invalid.
	lse.lsc.storeReportCommitBase(types.InvalidVersion, types.InvalidGLSN, varlogpb.LogSequenceNumber{
		LLSN: lastCommittedLLSN + 1,
		GLSN: uncommittedGLSNBegin,
	}, true /*invalid*/)

	// learning
	lse.resetInternalState(lastCommittedLLSN, !lse.isPrimary())
	lse.dstSyncInfo.lastSyncTime = time.Now()
	lse.dstSyncInfo.srcReplica = srcReplica.StorageNodeID
	return syncRange, nil
}

func (lse *Executor) SyncReplicate(_ context.Context, srcReplica varlogpb.LogStreamReplica, payload snpb.SyncPayload) (err error) {
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

	if lse.dstSyncInfo.srcReplica != srcReplica.StorageNodeID {
		return fmt.Errorf("log stream: sync replicate: incorrect source replica: %s", srcReplica.String())
	}

	if payload.LogEntry == nil && payload.CommitContext == nil {
		lse.esm.store(executorStateSealing)
		return fmt.Errorf("log stream: sync replicate: empty payload")
	}

	done := false
	batch := lse.stg.NewAppendBatch()
	defer func() {
		_ = batch.Close()
		if err != nil || done {
			lse.esm.store(executorStateSealing)
		}
	}()

	var lem *varlogpb.LogEntryMeta
	ver, hwm, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
	uncommittedLLSNBegin := uncommittedBegin.LLSN
	uncommittedGLSNBegin := uncommittedBegin.GLSN

	if entry := payload.LogEntry; entry != nil {
		if entry.LLSN != uncommittedLLSNBegin {
			err = fmt.Errorf("log stream: sync replicate: unexpected log entry: expected_llsn=%v, actual_llsn=%v", uncommittedLLSNBegin, entry.LLSN)
			return err
		}

		err = batch.SetLogEntry(entry.LLSN, entry.GLSN, entry.Data)
		if err != nil {
			return err
		}
		lse.logger.Info("log stream: sync replicate: copy", zap.String("log entry", entry.String()))
		uncommittedLLSNBegin = entry.LLSN + 1
		uncommittedGLSNBegin = entry.GLSN + 1
		lem = &varlogpb.LogEntryMeta{
			TopicID:     lse.tpid,
			LogStreamID: lse.lsid,
			LLSN:        entry.LLSN,
			GLSN:        entry.GLSN,
		}
	}
	if cc := payload.CommitContext; cc != nil {
		lastLLSN := cc.CommittedLLSNBegin + types.LLSN(cc.CommittedGLSNEnd-cc.CommittedGLSNBegin) - 1
		if lastLLSN != uncommittedLLSNBegin-1 {
			err = fmt.Errorf("log stream: sync replicate: unexpected commit context: expected_last_llsn=%v, actual_last_llsn=%v", uncommittedLLSNBegin-1, lastLLSN)
			return err
		}

		err = batch.SetCommitContext(storage.CommitContext{
			Version:            cc.Version,
			HighWatermark:      cc.HighWatermark,
			CommittedGLSNBegin: cc.CommittedGLSNBegin,
			CommittedGLSNEnd:   cc.CommittedGLSNEnd,
			CommittedLLSNBegin: cc.CommittedLLSNBegin,
		})
		if err != nil {
			return err
		}
		lse.logger.Info("log stream: sync replicate: copy", zap.String("commit context", cc.String()))

		ver = cc.Version
		hwm = cc.HighWatermark
		invalid = false
		done = true
	}
	err = batch.Apply()
	if err != nil {
		return err
	}

	if lem != nil {
		lse.lsc.localLWM.CompareAndSwap(varlogpb.LogSequenceNumber{}, varlogpb.LogSequenceNumber{
			LLSN: lem.LLSN,
			GLSN: lem.GLSN,
		})
	}
	lse.lsc.storeReportCommitBase(ver, hwm, varlogpb.LogSequenceNumber{
		LLSN: uncommittedLLSNBegin,
		GLSN: uncommittedGLSNBegin,
	}, invalid)
	lse.lsc.uncommittedLLSNEnd.Store(uncommittedLLSNBegin)
	lse.dstSyncInfo.lastSyncTime = time.Now()
	return nil
}
