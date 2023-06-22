package logstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/kakao/varlog/internal/storage"
	snerrors "github.com/kakao/varlog/internal/storagenode/errors"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

const (
	minQueueCapacity = 0
	maxQueueCapacity = 1 << 16

	singleflightKeyMetadata = "metadata"
)

type Executor struct {
	executorConfig
	lsc     *logStreamContext
	decider *decidableCondition
	esm     *executorStateManager
	sq      *sequencer
	wr      *writer
	cm      *committer
	bw      *backupWriter

	inflight       atomic.Int64
	inflightAppend atomic.Int64

	// FIXME: move to lsc
	globalLowWatermark struct {
		mu   sync.Mutex
		glsn types.GLSN
	}

	// muAdmin makes Seal, Unseal, SyncInit and SyncReplicate, Trim methods run mutually exclusively.
	muAdmin sync.Mutex
	// primaryBackups is a slice of replicas of a log stream.
	// It is updated by Unseal and is read by many codes.
	primaryBackups []varlogpb.LogStreamReplica
	dstSyncInfo    struct {
		// lastSyncTime is when the replica handles SyncInit or SyncReplicate
		// successfully. If the elapsed time since the last RPC is greater than
		// configured syncDurationTimeout, the synchronization process can be
		// canceled by another SyncInit.
		lastSyncTime time.Time
		srcReplica   types.StorageNodeID
	}
	sts        map[types.StorageNodeID]*syncTracker
	syncRunner *runner.Runner

	// replica connector for replication
	rcs *replicateClients

	// FIXME (jun): some metadata should be stored into the storage.
	createdTime time.Time

	// metric attributes
	metricAttrs []attribute.KeyValue

	// singleflight for metadata
	sf singleflight.Group

	// for debug
	prevUncommittedLLSNEnd types.AtomicLLSN
	prevCommitVersion      uint64
}

func NewExecutor(opts ...ExecutorOption) (lse *Executor, err error) {
	cfg, err := newExecutorConfig(opts)
	if err != nil {
		return nil, err
	}

	lse = &Executor{
		executorConfig: cfg,
		esm:            newExecutorStateManager(executorStateSealing),
		sts:            make(map[types.StorageNodeID]*syncTracker),
		createdTime:    time.Now(),
		metricAttrs: []attribute.KeyValue{
			attribute.Int("lsid", int(cfg.lsid)),
		},
	}

	lse.syncRunner = runner.New("sync", lse.logger.Named("sync"))

	rp, err := lse.stg.ReadRecoveryPoints()
	if err != nil {
		return nil, err
	}
	lse.lsc = lse.restoreLogStreamContext(rp)

	lse.decider = newDecidableCondition(lse.lsc)
	defer func() {
		if err == nil {
			lse.logger.Info("created")
			return
		}
		_ = lse.Close()
		lse = nil
	}()

	lse.sq, err = newSequencer(sequencerConfig{
		queueCapacity: lse.sequenceQueueCapacity,
		lse:           lse,
		logger:        lse.logger.Named("sequencer"),
	})
	if err != nil {
		return
	}

	lse.wr, err = newWriter(writerConfig{
		queueCapacity: lse.writeQueueCapacity,
		lse:           lse,
		logger:        lse.logger.Named("writer"),
	})
	if err != nil {
		return
	}

	lse.cm, err = newCommitter(committerConfig{
		commitQueueCapacity: lse.commitQueueCapacity,
		lse:                 lse,
		logger:              lse.logger.Named("committer"),
	})
	if err != nil {
		return
	}

	lse.bw, err = newBackupWriter(backupWriterConfig{
		queueCapacity: lse.writeQueueCapacity,
		lse:           lse,
		logger:        lse.logger.Named("backup writer"),
	})
	if err != nil {
		return
	}

	err = lse.restoreCommitWaitTasks(rp)
	if err != nil {
		return
	}

	return lse, err
}

func (lse *Executor) Replicate(ctx context.Context, llsnList []types.LLSN, dataList [][]byte) error {
	lse.inflight.Add(1)
	defer lse.inflight.Add(-1)

	switch lse.esm.load() {
	case executorStateSealing, executorStateSealed, executorStateLearning:
		return verrors.ErrSealed
	case executorStateClosed:
		return verrors.ErrClosed
	}

	if lse.isPrimary() {
		return errors.New("log stream: not backup")
	}

	var preparationDuration time.Duration
	startTime := time.Now()
	dataBytes := int64(0)
	batchSize := len(llsnList)
	defer func() {
		if lse.lsm == nil {
			return
		}
		lse.lsm.ReplicateLogs.Add(int64(batchSize))
		lse.lsm.ReplicateBytes.Add(dataBytes)
		lse.lsm.ReplicateDuration.Add(time.Since(startTime).Microseconds())
		lse.lsm.ReplicateOperations.Add(1)
		lse.lsm.ReplicatePreparationMicro.Add(preparationDuration.Microseconds())
	}()

	oldLLSN, newLLSN := llsnList[0], llsnList[batchSize-1]+1
	wb := lse.stg.NewWriteBatch()
	cwts := newListQueue()
	for i := 0; i < len(llsnList); i++ {
		_ = wb.Set(llsnList[i], dataList[i])
		dataBytes += int64(len(dataList[i]))
		cwts.PushFront(newCommitWaitTask(nil))
	}
	bwt := newBackupWriteTask(wb, oldLLSN, newLLSN)

	preparationDuration = time.Since(startTime)

	if err := lse.bw.send(ctx, bwt); err != nil {
		lse.logger.Error("could not send backup batch write task", zap.Error(err))
		_ = wb.Close()
		bwt.release()
		return err
	}

	if err := lse.cm.sendCommitWaitTask(ctx, cwts, false /*ignoreSealing*/); err != nil {
		lse.logger.Error("could not send commit wait task list", zap.Error(err))
		cwtListNode := cwts.Back()
		for cwtListNode != nil {
			cwt := cwtListNode.value.(*commitWaitTask)
			cwt.release()
			cwtListNode = cwtListNode.Prev()
		}
		return err
	}
	return nil
}

// Seal checks whether the log stream replica has committed the log entry
// confirmed lastly by the metadata repository. Since Trim can remove the last
// log entry, Seal compares the argument lastCommittedGLSN to the
// uncommittedBegin of the log stream context instead of the local high
// watermark.
//
// FIXME: No need to return localHWM.
func (lse *Executor) Seal(_ context.Context, lastCommittedGLSN types.GLSN) (status varlogpb.LogStreamStatus, localHWM types.GLSN, err error) {
	lse.inflight.Add(1)
	defer lse.inflight.Add(-1)

	lse.muAdmin.Lock()
	defer lse.muAdmin.Unlock()

	if lse.esm.load() == executorStateClosed {
		// FIXME: strange logstream status
		err = snerrors.ErrClosed
		return
	}

	lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
	if lse.esm.load() == executorStateSealed {
		return varlogpb.LogStreamStatusSealed, lastCommittedGLSN, nil
	}

	_, _, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
	if uncommittedBegin.GLSN-1 > lastCommittedGLSN {
		lse.logger.Panic("log stream: seal: metadata repository may be behind of log stream",
			zap.Any("local", uncommittedBegin.GLSN-1),
			zap.Any("mr", lastCommittedGLSN),
		)
	}
	if lastCommittedGLSN.Invalid() && invalid {
		// NOTE: Invalid last committed GLSN means this log stream just
		// joined into the cluster. However, an invalid
		// reportCommitBase means this replica has inconsistent commit
		// context and log entries; that can never happen for the new
		// log stream.
		lse.logger.Panic("log stream: seal: unexpected last committed GLSN")
	}

	_, localHighWatermark, _ := lse.lsc.localWatermarks()
	localHWM = localHighWatermark.GLSN

	if lse.esm.load() == executorStateLearning {
		return varlogpb.LogStreamStatusSealing, localHWM, nil
	}

	if uncommittedBegin.GLSN-1 < lastCommittedGLSN || invalid {
		status = varlogpb.LogStreamStatusSealing
		return status, localHWM, nil
	}
	status = varlogpb.LogStreamStatusSealed

	if lastCommittedGLSN.Invalid() {
		lse.esm.store(executorStateSealed)
		return status, localHWM, nil
	}

	lse.resetInternalState(uncommittedBegin.LLSN-1, true)

	lse.esm.store(executorStateSealed)
	return status, localHWM, nil
}

func (lse *Executor) Unseal(_ context.Context, replicas []varlogpb.LogStreamReplica) (err error) {
	lse.inflight.Add(1)
	defer lse.inflight.Add(-1)

	lse.muAdmin.Lock()
	defer lse.muAdmin.Unlock()

	if lse.esm.load() == executorStateClosed {
		return verrors.ErrClosed
	}

	err = varlogpb.ValidReplicas(replicas)
	if err != nil {
		return err
	}

	found := false
	for _, replica := range replicas {
		if replica.StorageNodeID == lse.snid && replica.LogStreamID == lse.lsid {
			found = true
			break
		}
	}
	if !found {
		return errors.New("no replica")
	}

	primary := replicas[0].StorageNodeID == lse.snid && replicas[0].LogStreamID == lse.lsid
	if primary {
		if lse.rcs != nil {
			lse.rcs.close()
		}
		lse.rcs = newReplicateClients()
		for i := 1; i < len(replicas); i++ {
			rpcConn, cerr := rpc.NewConn(context.Background(), replicas[i].Address, lse.replicateClientGRPCOptions...)
			if cerr != nil {
				return cerr
			}
			client, cerr := newReplicateClient(context.Background(), replicateClientConfig{
				replica:       replicas[i],
				rpcConn:       rpcConn,
				queueCapacity: lse.replicateClientQueueCapacity,
				//grpcDialOptions: lse.replicateClientGRPCOptions,
				lse:    lse,
				logger: lse.logger.Named("replicate client"),
			})
			if cerr != nil {
				return cerr
			}
			lse.rcs.add(client)
		}
	}
	if err != nil {
		return err
	}

	lse.esm.compareAndSwap(executorStateSealed, executorStateAppendable)
	if state := lse.esm.load(); state != executorStateAppendable {
		return fmt.Errorf("log stream: unseal: state not ready %v", state)
	}
	lse.primaryBackups = replicas
	return nil
}

func (lse *Executor) resetInternalState(lastCommittedLLSN types.LLSN, discardCommitWaitTasks bool) {
	if ce := lse.logger.Check(zap.DebugLevel, "resetting internal state"); ce != nil {
		ce.Write(
			zap.Int64("inflight", lse.inflight.Load()),
			zap.Int64("inflight_append", lse.inflightAppend.Load()),
		)
	}

	// close replicClients in replica connector
	lse.rcs.close()

	// sequencer
	lse.sq.waitForDrainage(verrors.ErrSealed, false)

	// writer
	lse.wr.waitForDrainage(verrors.ErrSealed, false)

	// backup writer
	lse.bw.waitForDrainage(false)

	// committer
	lse.cm.waitForDrainageOfCommitQueue(false)
	if discardCommitWaitTasks {
		lse.cm.drainCommitWaitQ(verrors.ErrSealed)
	}

	// TODO: delete storage? really?

	// reset llsn
	lse.sq.llsn = lastCommittedLLSN

	// log stream context
	lse.lsc.uncommittedLLSNEnd.Store(lastCommittedLLSN + 1)
}

func (lse *Executor) Report(_ context.Context) (report snpb.LogStreamUncommitReport, err error) {
	lse.inflight.Add(1)
	defer lse.inflight.Add(-1)

	if lse.esm.load() == executorStateClosed {
		return snpb.LogStreamUncommitReport{}, verrors.ErrClosed
	}

	version, highWatermark, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
	if invalid {
		// If it is invalid, incompatibility between the commit context
		// and the last log entry happens. The report must contain an
		// invalid version, an invalid high watermark, and an invalid
		// uncommittedLLSNOffset, and the metadata repository ignores
		// invalid reports.
		report.LogStreamID = lse.lsid
		return report, nil
	}

	uncommittedLLSNBegin := uncommittedBegin.LLSN
	uncommittedLLSNEnd := lse.lsc.uncommittedLLSNEnd.Load()
	report = snpb.LogStreamUncommitReport{
		LogStreamID:           lse.lsid,
		Version:               version,
		HighWatermark:         highWatermark,
		UncommittedLLSNOffset: uncommittedLLSNBegin,
		UncommittedLLSNLength: uint64(uncommittedLLSNEnd - uncommittedLLSNBegin),
	}
	prevUncommittedLLSNEnd := lse.prevUncommittedLLSNEnd.Load()
	if prevUncommittedLLSNEnd != uncommittedLLSNEnd {
		if ce := lse.logger.Check(zap.DebugLevel, "log stream: report"); ce != nil {
			ce.Write(zap.Any("report", report))
		}
		lse.prevUncommittedLLSNEnd.Store(uncommittedLLSNEnd)
	}

	if lse.esm.load() == executorStateLearning {
		return snpb.LogStreamUncommitReport{}, errors.New("log stream: learning state")
	}
	return report, nil
}

func (lse *Executor) Commit(ctx context.Context, commitResult snpb.LogStreamCommitResult) error {
	lse.inflight.Add(1)
	defer lse.inflight.Add(-1)

	if lse.esm.load() == executorStateClosed {
		return verrors.ErrClosed
	}

	version, _, _, invalid := lse.lsc.reportCommitBase()
	if commitResult.Version <= version {
		return errors.New("too old commit result")
	}
	if invalid {
		return errors.New("invalid replica status")
	}

	if types.Version(atomic.LoadUint64(&lse.prevCommitVersion)) != commitResult.Version {
		if ce := lse.logger.Check(zap.DebugLevel, "commit"); ce != nil {
			ce.Write(zap.String("commit_result", commitResult.String()))
		}
		atomic.StoreUint64(&lse.prevCommitVersion, uint64(commitResult.Version))
	}

	ct := newCommitTask()
	ct.version = commitResult.Version
	ct.highWatermark = commitResult.HighWatermark
	ct.committedGLSNBegin = commitResult.CommittedGLSNOffset
	ct.committedGLSNEnd = commitResult.CommittedGLSNOffset + types.GLSN(commitResult.CommittedGLSNLength)
	ct.committedLLSNBegin = commitResult.CommittedLLSNOffset
	if err := lse.cm.sendCommitTask(ctx, ct); err != nil {
		ct.release()
		return err
	}
	return nil
}

func (lse *Executor) Metadata() (snpb.LogStreamReplicaMetadataDescriptor, error) {
	lse.inflight.Add(1)
	defer lse.inflight.Add(-1)

	ret, err, _ := lse.sf.Do(singleflightKeyMetadata, func() (interface{}, error) {
		state := lse.esm.load()
		if state == executorStateClosed {
			return snpb.LogStreamReplicaMetadataDescriptor{}, verrors.ErrClosed
		}
		return lse.metadataDescriptor(state), nil
	})
	return ret.(snpb.LogStreamReplicaMetadataDescriptor), err
}

func (lse *Executor) metadataDescriptor(state executorState) snpb.LogStreamReplicaMetadataDescriptor {
	var status varlogpb.LogStreamStatus
	switch state {
	case executorStateAppendable:
		status = varlogpb.LogStreamStatusRunning
	case executorStateSealing, executorStateLearning:
		status = varlogpb.LogStreamStatusSealing
	case executorStateSealed:
		status = varlogpb.LogStreamStatusSealed
	case executorStateClosed:
		// FIXME: It is not correct status
		status = varlogpb.LogStreamStatusSealed
	}

	localLowWatermark, localHighWatermark, _ := lse.lsc.localWatermarks()
	version, globalHighWatermark, _, _ := lse.lsc.reportCommitBase()
	return snpb.LogStreamReplicaMetadataDescriptor{
		LogStreamReplica: varlogpb.LogStreamReplica{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.snid,
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     lse.tpid,
				LogStreamID: lse.lsid,
			},
		},
		Version:             version,
		GlobalHighWatermark: globalHighWatermark,
		LocalLowWatermark: varlogpb.LogSequenceNumber{
			LLSN: localLowWatermark.LLSN,
			GLSN: localLowWatermark.GLSN,
		},
		LocalHighWatermark: varlogpb.LogSequenceNumber{
			LLSN: localHighWatermark.LLSN,
			GLSN: localHighWatermark.GLSN,
		},
		Status:           status,
		Path:             lse.stg.Path(),
		StorageSizeBytes: lse.stg.DiskUsage(),
		CreatedTime:      lse.createdTime,
	}
}

func (lse *Executor) Trim(_ context.Context, glsn types.GLSN) error {
	lse.inflight.Add(1)
	defer lse.inflight.Add(-1)

	lse.muAdmin.Lock()
	defer lse.muAdmin.Unlock()

	switch lse.esm.load() {
	case executorStateSealing, executorStateSealed, executorStateLearning:
		return verrors.ErrSealed
	case executorStateClosed:
		return verrors.ErrClosed
	}

	// NB: When a replica is started just ago, it may not know the global high watermark.
	// It means that it can return an error accidentally.
	// It can fix by allowing TrimDeprecated RPC only when the replica is executorStateAppendable.
	_, globalHWM, _, _ := lse.lsc.reportCommitBase()
	if glsn > globalHWM {
		// not appended yet
		return fmt.Errorf("log stream: trim: %d not appended, global high watermark %d", glsn, globalHWM)
	}

	localLowWatermark, localHighWatermark, _ := lse.lsc.localWatermarks()
	if localHighWatermark.Invalid() || glsn < localLowWatermark.GLSN {
		// already trimmed
		return nil
	}

	var nextLocalLWM varlogpb.LogSequenceNumber
	if localHighWatermark.GLSN <= glsn {
		// delete all log entries, so no local lwm
		nextLocalLWM = varlogpb.LogSequenceNumber{LLSN: types.InvalidLLSN, GLSN: types.InvalidGLSN}
	} else {
		// find next low watermark
		sr, err := lse.SubscribeWithGLSN(glsn+1, localHighWatermark.GLSN+1)
		if err != nil {
			return fmt.Errorf("log stream: trim: %w", err)
		}
		entry, ok := <-sr.Result()
		if !ok {
			return fmt.Errorf("log stream: trim: too high to trim %d", glsn)
		}
		sr.Stop()

		nextLocalLWM = varlogpb.LogSequenceNumber{
			LLSN: entry.LLSN,
			GLSN: entry.GLSN,
		}
	}

	if err := lse.stg.Trim(glsn); err != nil {
		return err
	}

	lse.lsc.setLocalLowWatermark(nextLocalLWM)

	// update global low watermark
	lse.globalLowWatermark.mu.Lock()
	defer lse.globalLowWatermark.mu.Unlock()
	lse.globalLowWatermark.glsn = glsn + 1

	return nil
}

// Path returns the data directory where the replica stores its data.
func (lse *Executor) Path() string {
	return lse.stg.Path()
}

func (lse *Executor) Metrics() *telemetry.LogStreamMetrics {
	return lse.lsm
}

func (lse *Executor) Close() (err error) {
	lse.esm.store(executorStateClosed)
	lse.rcs.close()
	if lse.cm != nil {
		lse.cm.stop()
	}
	if lse.wr != nil {
		lse.wr.stop()
	}
	if lse.sq != nil {
		lse.sq.stop()
	}
	if lse.bw != nil {
		lse.bw.stop()
	}
	if lse.stg != nil {
		err = lse.stg.Close()
	}
	lse.decider.destroy()
	lse.waitForDrainage()
	lsrmd := lse.metadataDescriptor(lse.esm.load())
	lse.logger.Info("closed",
		zap.String("local_lwm", lsrmd.LocalLowWatermark.String()),
		zap.String("local_hwm", lsrmd.LocalHighWatermark.String()),
		zap.Uint64("global_hwm", uint64(lsrmd.GlobalHighWatermark)),
	)
	return err
}

func (lse *Executor) waitForDrainage() {
	const tick = time.Millisecond
	timer := time.NewTimer(tick)
	defer timer.Stop()

	for lse.inflight.Load() > 0 {
		<-timer.C
		timer.Reset(tick)
	}
}

func (lse *Executor) isPrimary() bool {
	// NOTE: A new log stream replica that has not received Unseal is not primary replica.
	return len(lse.primaryBackups) > 0 && lse.primaryBackups[0].StorageNodeID == lse.snid && lse.primaryBackups[0].LogStreamID == lse.lsid
}

func (lse *Executor) restoreLogStreamContext(rp storage.RecoveryPoints) *logStreamContext {
	cc := rp.LastCommitContext
	first := rp.CommittedLogEntry.First
	last := rp.CommittedLogEntry.Last

	lsc := newLogStreamContext()
	restoreMode := "init"
	defer func() {
		lse.logger.Info("restore log stream context", zap.String("mode", restoreMode))
	}()

	// Log stream replica that has not committed any logs yet. For example, a
	// new log stream replica.
	if cc == nil && last == nil {
		uncommittedLLSNEnd := lsc.uncommittedLLSNEnd.Load()
		if !rp.UncommittedLLSN.End.Invalid() {
			uncommittedLLSNEnd = rp.UncommittedLLSN.End
		}
		lsc.uncommittedLLSNEnd.Store(uncommittedLLSNEnd)

		return lsc
	}

	// Log stream replica with a commit context but no log entry. For instance,
	// the log stream replica that trimmed all log entries.
	if cc != nil && last == nil {
		restoreMode = "recovered"
		uncommittedLLSNBegin := cc.CommittedLLSNBegin + types.LLSN(cc.CommittedGLSNEnd-cc.CommittedGLSNBegin)
		uncommittedBegin := varlogpb.LogSequenceNumber{
			LLSN: uncommittedLLSNBegin,
			GLSN: cc.CommittedGLSNEnd,
		}
		lsc.storeReportCommitBase(cc.Version, cc.HighWatermark, uncommittedBegin, false /*invalid*/)

		uncommittedLLSNEnd := uncommittedLLSNBegin
		if !rp.UncommittedLLSN.End.Invalid() {
			uncommittedLLSNEnd = rp.UncommittedLLSN.End
		}
		lsc.uncommittedLLSNEnd.Store(uncommittedLLSNEnd)

		return lsc
	}

	// Log stream replica that has a commit context and log entries. The commit
	// context specifies the last log entry exactly.
	if cc != nil && last != nil {
		restoreMode = "recovered"
		uncommittedLLSNBegin := cc.CommittedLLSNBegin + types.LLSN(cc.CommittedGLSNEnd-cc.CommittedGLSNBegin)
		if uncommittedLLSNBegin-1 == last.LLSN {
			uncommittedBegin := varlogpb.LogSequenceNumber{
				LLSN: last.LLSN + 1,
				GLSN: last.GLSN + 1,
			}
			lsc.storeReportCommitBase(cc.Version, cc.HighWatermark, uncommittedBegin, false /*invalid*/)

			uncommittedLLSNEnd := uncommittedLLSNBegin
			if !rp.UncommittedLLSN.End.Invalid() {
				uncommittedLLSNEnd = rp.UncommittedLLSN.End
			}
			lsc.uncommittedLLSNEnd.Store(uncommittedLLSNEnd)

			lsc.setLocalLowWatermark(varlogpb.LogSequenceNumber{
				LLSN: first.LLSN,
				GLSN: first.GLSN,
			})
			return lsc
		}
	}

	// Log stream replica is invalid:
	//
	// - It has committed log entries but no commit context.
	// - It has a commit context and committed log entries, but the commit
	//   context doesn't specify the last log entry.
	//
	// Invalid log stream replica should be resolved by synchronization from
	// the sealed source replica. Log stream context should be set carefully to
	// receive copies of log entries through synchronization. To restore the
	// log stream replica restarted during the synchronization phase, the log
	// stream context is initiated by the committed log entries.
	restoreMode = "invalid"
	lsc.storeReportCommitBase(types.InvalidVersion, types.InvalidGLSN, varlogpb.LogSequenceNumber{}, true /*invalid*/)
	if last != nil {
		uncommittedBegin := varlogpb.LogSequenceNumber{
			LLSN: last.LLSN + 1,
			GLSN: last.GLSN + 1,
		}
		lsc.storeReportCommitBase(types.InvalidVersion, types.InvalidGLSN, uncommittedBegin, true /*invalid*/)
		lsc.uncommittedLLSNEnd.Store(last.LLSN + 1)
		lsc.setLocalLowWatermark(varlogpb.LogSequenceNumber{
			LLSN: first.LLSN,
			GLSN: first.GLSN,
		})
	}
	return lsc
}

func (lse *Executor) restoreCommitWaitTasks(rp storage.RecoveryPoints) error {
	// Invalid log stream replica does not receive commit messages from the metadata repository since synchronization can resolve it only.
	_, _, _, invalid := lse.lsc.reportCommitBase()
	if invalid {
		return nil
	}

	// No uncommitted logs, so no CommitWaitTasks.
	if rp.UncommittedLLSN.Begin.Invalid() {
		return nil
	}

	cwts := newListQueue()
	for i := rp.UncommittedLLSN.Begin; i < rp.UncommittedLLSN.End; i++ {
		cwts.PushFront(newCommitWaitTask(nil))
	}
	err := lse.cm.sendCommitWaitTask(context.Background(), cwts, true /*ignoreSealing*/)
	if err != nil {
		lse.logger.Error("could not send commit wait task list", zap.Error(err))
		cwtListNode := cwts.Back()
		for cwtListNode != nil {
			cwt := cwtListNode.value.(*commitWaitTask)
			cwt.release()
			cwtListNode = cwtListNode.Prev()
		}
		return err
	}
	return nil
}
