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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	localLWM, localHWM, _ := lse.lsc.localWatermarks()

	// The committedLLSNEnd is types.MinLLSN if the result of ReadCommitContext
	// is storage.ErrNoCommitContext, since it means this log stream has not
	// received a commit message from the metadata repository yet.
	committedLLSNEnd := types.MinLLSN
	if cc, err := lse.stg.ReadCommitContext(); err == nil {
		committedLLSNEnd = cc.CommittedLLSNBegin + types.LLSN(cc.CommittedGLSNEnd-cc.CommittedGLSNBegin)
	} else if err != storage.ErrNoCommitContext {
		return nil, status.Errorf(codes.Internal, "sync: %s", err.Error())
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

	syncRange, err := sc.syncInit(ctx, snpb.SyncRange{
		FirstLLSN: localLWM.LLSN,
		LastLLSN:  localHWM.LLSN,
	}, committedLLSNEnd-1)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			err = nil
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
		return nil, err
	}

	// If the FirstLLSN of the sync range is greater than the LastLLSN of
	// it, the destination has all log entries but commit context.
	var first varlogpb.LogEntry
	if !syncRange.FirstLLSN.Invalid() && syncRange.FirstLLSN <= syncRange.LastLLSN {
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

func (lse *Executor) SyncInit(_ context.Context, srcReplica varlogpb.LogStreamReplica, srcRange snpb.SyncRange, srcLastCommittedLLSN types.LLSN) (syncRange snpb.SyncRange, err error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	lse.muAdmin.Lock()
	defer lse.muAdmin.Unlock()

	err = srcRange.Validate()
	if err != nil {
		err = status.Error(codes.InvalidArgument, err.Error())
		return
	}

	if !srcRange.LastLLSN.Invalid() && srcRange.LastLLSN != srcLastCommittedLLSN {
		err = status.Errorf(codes.InvalidArgument, "unmatched llsn: the last of range %d, the last committed llsn %d", srcRange.LastLLSN, srcLastCommittedLLSN)
		return
	}

	if state := lse.esm.load(); state != executorStateSealing {
		if state == executorStateClosed {
			err = fmt.Errorf("log stream: sync init: %w", verrors.ErrClosed)
			return
		}
		if state != executorStateLearning {
			err = fmt.Errorf("log stream: sync init: invalid state %d: %w", state, verrors.ErrInvalid)
			return
		}
		if elapsed := time.Since(lse.dstSyncInfo.lastSyncTime); elapsed < lse.syncTimeout {
			err = fmt.Errorf("log stream: sync init: learning in-progress %s", elapsed)
			return
		}
		// syncTimeout is expired.
		lse.esm.store(executorStateSealing)
	}

	_, _, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
	uncommittedLLSNBegin, _ := uncommittedBegin.LLSN, uncommittedBegin.GLSN
	dstLastCommittedLLSN := uncommittedLLSNBegin - 1
	if !srcRange.LastLLSN.Invalid() && srcRange.LastLLSN < dstLastCommittedLLSN {
		lse.logger.Panic("sync init: destination of sync has too many logs",
			zap.String("src_range", srcRange.String()),
			zap.Uint64("last_committed_llsn", uint64(dstLastCommittedLLSN)),
		)
	}

	// NOTE: When the replica has all log entries, it returns its range of logs and non-error results.
	// In this case, this replica remains executorStateSealing.
	// Breaking change: previously it returns ErrExist when the replica has all log entries to replicate.
	if dstLastCommittedLLSN == srcRange.LastLLSN && !invalid {
		return snpb.SyncRange{}, status.Errorf(codes.AlreadyExists, "already synchronized")
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
	alreadySynchronized := false

	switch {
	case srcRange.FirstLLSN.Invalid() && srcRange.LastLLSN.Invalid():
		if srcLastCommittedLLSN < dstLastCommittedLLSN {
			lse.logger.Panic("sync init: destination of sync had too many logs",
				zap.Any("src_last_committed_llsn", srcLastCommittedLLSN),
				zap.Any("dst_last_committed_llsn", dstLastCommittedLLSN),
			)
		} else if srcLastCommittedLLSN == dstLastCommittedLLSN && !invalid {
			alreadySynchronized = true
		}

		// The source replica does not have log entries when both the FirstLLSN
		// and LastLLSN of the srcRange are InvalidLLSNs. Therefore, the
		// destination replica must remove all log entries.
		trimGLSN = types.MaxGLSN
		lwm = varlogpb.LogSequenceNumber{LLSN: types.InvalidLLSN, GLSN: types.InvalidGLSN}
		uncommittedBegin = varlogpb.LogSequenceNumber{
			LLSN: srcLastCommittedLLSN,
			GLSN: types.InvalidGLSN, // It is set to InvalidGLSN since it cannot be known.
		}

		syncRange = snpb.SyncRange{
			FirstLLSN: types.InvalidLLSN,
			LastLLSN:  types.InvalidLLSN,
		}

	case srcRange.FirstLLSN < lwm.LLSN:
		// The destination replica has a higher LowWatermark than the FirstLLSN
		// of the SyncRange sent from the source replica, meaning the
		// destination replica might have already been trimmed.
		// To simplify the synchronization process, log entries in the
		// destination replica will be cut, and then, the log entries will be
		// copied from the source to the destination.
		trimGLSN = types.MaxGLSN
		lwm = varlogpb.LogSequenceNumber{LLSN: types.InvalidLLSN, GLSN: types.InvalidGLSN}
		uncommittedBegin = varlogpb.LogSequenceNumber{
			LLSN: srcRange.FirstLLSN,
			GLSN: types.InvalidGLSN, // It is set to InvalidGLSN since it cannot be known.
		}

		syncRange = snpb.SyncRange{
			FirstLLSN: srcRange.FirstLLSN,
			LastLLSN:  srcRange.LastLLSN,
		}

	case srcRange.FirstLLSN == lwm.LLSN:
		// The destination replica must not remove log entries; therefore, it
		// does not need to change the local low watermark.
		// no need to trim
		trimGLSN = types.InvalidGLSN
		syncRange = snpb.SyncRange{
			FirstLLSN: dstLastCommittedLLSN + 1,
			LastLLSN:  srcRange.LastLLSN,
		}

	case lwm.LLSN < srcRange.FirstLLSN && srcRange.FirstLLSN <= dstLastCommittedLLSN:
		// The destination replica has to trim log entries lower than the
		// FirstLLSN of the SyncRange because the source replica did.
		// The local low watermark of the destination replica also has to be
		// updated.
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
		lwm = varlogpb.LogSequenceNumber{
			LLSN: entry.LLSN,
			GLSN: entry.GLSN,
		}

		syncRange = snpb.SyncRange{
			FirstLLSN: dstLastCommittedLLSN + 1,
			LastLLSN:  srcRange.LastLLSN,
		}

	default: // dstLastCommittedLLSN < srcRange.FirstLLSN
		// All log entries in the destination replica should be removed since
		// the log entries in the source replica have already been removed. So
		// the local low watermark in the destination replica has to be
		// changed.
		trimGLSN = types.MaxGLSN
		lwm = varlogpb.LogSequenceNumber{LLSN: types.InvalidLLSN, GLSN: types.InvalidGLSN}
		uncommittedBegin = varlogpb.LogSequenceNumber{
			LLSN: srcRange.FirstLLSN,
			GLSN: types.InvalidGLSN, // It is set to InvalidGLSN since it cannot be known.
		}

		syncRange = snpb.SyncRange{
			FirstLLSN: srcRange.FirstLLSN,
			LastLLSN:  srcRange.LastLLSN,
		}
	}

	// It is unnecessary to copy log entries if FirstLLSN is greater than
	// LastLLSN, and it has to copy only the commit context.
	if syncRange.FirstLLSN > syncRange.LastLLSN {
		syncRange = snpb.SyncRange{
			FirstLLSN: types.InvalidLLSN,
			LastLLSN:  types.InvalidLLSN,
		}
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

	if alreadySynchronized {
		lse.esm.store(executorStateSealing)
		return snpb.SyncRange{}, status.Errorf(codes.AlreadyExists, "already synchronized")
	}

	// NOTE: Invalid reportCommitBase makes the report of the log
	// stream replica meaningless.
	// The LLSN of uncommittedBegin indicates a sequence number of the
	// following log entry copied from a source replica.
	// Invalid GLSN of uncommittedBegin makes the local high watermark of the
	// replica invalid.
	lse.lsc.storeReportCommitBase(types.InvalidVersion, types.InvalidGLSN, uncommittedBegin, true /*invalid*/)

	// learning
	lse.resetInternalState(dstLastCommittedLLSN, !lse.isPrimary())
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
