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

	inflight       int64
	inflightAppend int64

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
	// sync replicate buffer
	srb        *syncReplicateBuffer
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

	return lse, err
}

func (lse *Executor) Replicate(ctx context.Context, llsnList []types.LLSN, dataList [][]byte) error {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

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
		atomic.AddInt64(&lse.lsm.ReplicateLogs, int64(batchSize))
		atomic.AddInt64(&lse.lsm.ReplicateBytes, dataBytes)
		atomic.AddInt64(&lse.lsm.ReplicateDuration, time.Since(startTime).Microseconds())
		atomic.AddInt64(&lse.lsm.ReplicateOperations, 1)
		atomic.AddInt64(&lse.lsm.ReplicatePreparationMicro, preparationDuration.Microseconds())
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

	if err := lse.cm.sendCommitWaitTask(ctx, cwts); err != nil {
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

func (lse *Executor) Seal(_ context.Context, lastCommittedGLSN types.GLSN) (status varlogpb.LogStreamStatus, localHWM types.GLSN, err error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	lse.muAdmin.Lock()
	defer lse.muAdmin.Unlock()

	if lse.esm.load() == executorStateClosed {
		// FIXME: strange logstream status
		err = verrors.ErrClosed
		return
	}

	lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
	if lse.esm.load() == executorStateSealed {
		return varlogpb.LogStreamStatusSealed, lastCommittedGLSN, nil
	}

	localHighWatermark := lse.lsc.localHighWatermark()
	localHWM = localHighWatermark.GLSN
	if localHighWatermark.GLSN > lastCommittedGLSN {
		panic("log stream: seal: metadata repository may be behind of log stream")
	}
	if localHighWatermark.GLSN < lastCommittedGLSN {
		status = varlogpb.LogStreamStatusSealing
		return status, localHWM, nil
	}
	status = varlogpb.LogStreamStatusSealed

	if lastCommittedGLSN.Invalid() {
		lse.esm.store(executorStateSealed)
		return status, localHWM, nil
	}

	lse.resetInternalState(localHighWatermark.LLSN, true)

	lse.esm.store(executorStateSealed)
	return status, localHWM, nil
}

func (lse *Executor) Unseal(_ context.Context, replicas []varlogpb.LogStreamReplica) (err error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

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
	lse.logger.Debug("resetting internal state",
		zap.Int64("inflight", atomic.LoadInt64(&lse.inflight)),
		zap.Int64("inflight_append", atomic.LoadInt64(&lse.inflightAppend)),
	)

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

func (lse *Executor) Report(_ context.Context) (snpb.LogStreamUncommitReport, error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	if lse.esm.load() == executorStateClosed {
		return snpb.LogStreamUncommitReport{}, verrors.ErrClosed
	}

	version, highWatermark, uncommittedLLSNBegin := lse.lsc.reportCommitBase()
	uncommittedLLSNEnd := lse.lsc.uncommittedLLSNEnd.Load()
	report := snpb.LogStreamUncommitReport{
		LogStreamID:           lse.lsid,
		Version:               version,
		HighWatermark:         highWatermark,
		UncommittedLLSNOffset: uncommittedLLSNBegin,
		UncommittedLLSNLength: uint64(uncommittedLLSNEnd - uncommittedLLSNBegin),
	}
	prevUncommittedLLSNEnd := lse.prevUncommittedLLSNEnd.Load()
	if prevUncommittedLLSNEnd != uncommittedLLSNEnd {
		lse.logger.Debug("log stream: report", zap.Any("report", report))
		lse.prevUncommittedLLSNEnd.Store(uncommittedLLSNEnd)
	}
	return report, nil
}

func (lse *Executor) Commit(ctx context.Context, commitResult snpb.LogStreamCommitResult) error {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	if lse.esm.load() == executorStateClosed {
		return verrors.ErrClosed
	}

	version, _, _ := lse.lsc.reportCommitBase()
	if commitResult.Version <= version {
		return errors.New("too old commit result")
	}

	if types.Version(atomic.LoadUint64(&lse.prevCommitVersion)) != commitResult.Version {
		lse.logger.Debug("commit", zap.String("commit_result", commitResult.String()))
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
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

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

	localLowWatermark := lse.lsc.localLowWatermark()
	localHighWatermark := lse.lsc.localHighWatermark()
	version, globalHighWatermark, _ := lse.lsc.reportCommitBase()
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

func (lse *Executor) LogStreamMetadata() (lsd varlogpb.LogStreamDescriptor, err error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	if lse.esm.load() == executorStateClosed {
		return lsd, verrors.ErrClosed
	}

	var status varlogpb.LogStreamStatus
	switch lse.esm.load() {
	case executorStateAppendable:
		status = varlogpb.LogStreamStatusRunning
	case executorStateSealing, executorStateLearning:
		status = varlogpb.LogStreamStatusSealing
	case executorStateSealed:
		status = varlogpb.LogStreamStatusSealed
	}

	localLWM := lse.lsc.localLowWatermark()
	localLWM.TopicID = lse.tpid
	localLWM.LogStreamID = lse.lsid

	localHWM := lse.lsc.localHighWatermark()
	localHWM.TopicID = lse.tpid
	localHWM.LogStreamID = lse.lsid

	lsd = varlogpb.LogStreamDescriptor{
		TopicID:     lse.tpid,
		LogStreamID: lse.lsid,
		Status:      status,
		Head:        localLWM,
		Tail:        localHWM,
	}
	return lsd, nil
}

func (lse *Executor) Trim(_ context.Context, glsn types.GLSN) error {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

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
	_, globalHWM, _ := lse.lsc.reportCommitBase()
	if glsn > globalHWM {
		// not appended yet
		return fmt.Errorf("log stream: trim: %d not appended, global high watermark %d", glsn, globalHWM)
	}

	const safetyGap = 1
	localHighWatermark := lse.lsc.localHighWatermark()
	if glsn > localHighWatermark.GLSN-safetyGap {
		return fmt.Errorf("log stream: trim: too high to trim %d, local high watermark %d, safety gap: %d",
			glsn, localHighWatermark.GLSN, safetyGap)
	}

	// find next low watermark
	sr, err := lse.SubscribeWithGLSN(glsn+1, localHighWatermark.GLSN+1)
	if err != nil {
		return fmt.Errorf("log stream: trim: %w", err)
	}
	nextLowWatermark, ok := <-sr.Result()
	sr.Stop()
	if !ok {
		return fmt.Errorf("log stream: trim: too high to trim %d", glsn)
	}

	lse.globalLowWatermark.mu.Lock()
	if glsn < lse.globalLowWatermark.glsn {
		lse.globalLowWatermark.mu.Unlock()
		// already trimmed
		return nil
	}
	lse.globalLowWatermark.mu.Unlock()

	if err := lse.stg.Trim(glsn); err != nil {
		return err
	}

	// update global low watermark
	lse.globalLowWatermark.mu.Lock()
	defer lse.globalLowWatermark.mu.Unlock()
	lse.globalLowWatermark.glsn = glsn + 1

	// update local low watermark
	lse.lsc.setLocalLowWatermark(nextLowWatermark.LogEntryMeta)
	return nil
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

	for atomic.LoadInt64(&lse.inflight) > 0 {
		select {
		case <-timer.C:
			timer.Reset(tick)
		}
	}
}

func (lse *Executor) isPrimary() bool {
	// NOTE: A new log stream replica that has not received Unseal is not primary replica.
	return len(lse.primaryBackups) > 0 && lse.primaryBackups[0].StorageNodeID == lse.snid && lse.primaryBackups[0].LogStreamID == lse.lsid
}

func (lse *Executor) restoreLogStreamContext(recoveryPoints storage.RecoveryPoints) *logStreamContext {
	lsc := newLogStreamContext()
	commitVersion, globalHighWatermark, uncommittedLLSNBegin := lsc.reportCommitBase()
	uncommittedLLSNEnd := lsc.uncommittedLLSNEnd.Load()
	localLowWatermark := lsc.localLowWatermark()
	localHighWatermark := lsc.localHighWatermark()

	if lastCC := recoveryPoints.LastCommitContext; lastCC != nil {
		commitVersion = lastCC.Version
		globalHighWatermark = lastCC.HighWatermark
	}
	if boundaries := recoveryPoints.CommittedLogEntry; boundaries.First != nil {
		lastLLSN := boundaries.Last.LLSN
		uncommittedLLSNBegin = lastLLSN + 1
		uncommittedLLSNEnd = lastLLSN + 1
		localLowWatermark = *boundaries.First
		localHighWatermark = *boundaries.Last
	}

	lsc.storeReportCommitBase(commitVersion, globalHighWatermark, uncommittedLLSNBegin)
	lsc.uncommittedLLSNEnd.Store(uncommittedLLSNEnd)
	lsc.setLocalLowWatermark(localLowWatermark)
	lsc.setLocalHighWatermark(localHighWatermark)
	return lsc
}
