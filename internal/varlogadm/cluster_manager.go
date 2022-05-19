package varlogadm

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/varlogadm -package varlogadm -destination varlogadm_mock.go . StorageNodeWatcher,StatRepository

import (
	"context"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/client"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/netutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
)

type StorageNodeEventHandler interface {
	HandleHeartbeatTimeout(context.Context, types.StorageNodeID)
	HandleReport(context.Context, *snpb.StorageNodeMetadataDescriptor, time.Duration)
}

const numLogStreamMutex = 512

type ClusterManager struct {
	config

	closed       bool
	lis          net.Listener
	server       *grpc.Server
	serverAddr   string
	healthServer *health.Server

	// single large lock
	mu                sync.RWMutex
	muLogStreamStatus [numLogStreamMutex]sync.Mutex

	snSelector     ReplicaSelector
	snWatcher      StorageNodeWatcher
	statRepository StatRepository
	logStreamIDGen *LogStreamIDGenerator
	topicIDGen     *TopicIDGenerator
}

func NewClusterManager(ctx context.Context, opts ...Option) (*ClusterManager, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	cmView := cfg.mrMgr.ClusterMetadataView()

	logStreamIDGen, err := NewLogStreamIDGenerator(ctx, cmView)
	if err != nil {
		return nil, err
	}

	topicIDGen, err := NewTopicIDGenerator(ctx, cmView)
	if err != nil {
		return nil, err
	}

	snSelector, err := newBalancedReplicaSelector(cmView, int(cfg.replicationFactor))
	if err != nil {
		return nil, err
	}

	cm := &ClusterManager{
		config:         cfg,
		snSelector:     snSelector,
		statRepository: NewStatRepository(ctx, cmView),
		logStreamIDGen: logStreamIDGen,
		topicIDGen:     topicIDGen,
	}

	cm.snWatcher = NewStorageNodeWatcher(cfg.watcherOptions, cmView, cfg.snMgr, cm, cm.logger)
	cm.server = grpc.NewServer()
	cm.healthServer = health.NewServer()

	return cm, nil
}

func (cm *ClusterManager) Address() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.serverAddr
}

func (cm *ClusterManager) Serve() error {
	cm.mu.Lock()
	if cm.lis != nil {
		cm.mu.Unlock()
		return errors.New("admin: already serving")
	}
	lis, err := net.Listen("tcp", cm.listenAddress)
	if err != nil {
		cm.mu.Unlock()
		return errors.WithStack(err)
	}
	cm.lis = lis
	addrs, _ := netutil.GetListenerAddrs(lis.Addr())
	cm.serverAddr = addrs[0]

	newClusterManagerService(cm, cm.logger).Register(cm.server)
	grpc_health_v1.RegisterHealthServer(cm.server, cm.healthServer)
	cm.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// SN Watcher
	if err := cm.snWatcher.Serve(); err != nil {
		cm.mu.Unlock()
		return err
	}
	cm.mu.Unlock()

	return cm.server.Serve(lis)
}

func (cm *ClusterManager) Close() (err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.closed {
		return nil
	}
	cm.closed = true

	cm.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// SN Watcher
	err = cm.snWatcher.Close()
	err = multierr.Combine(err, cm.snMgr.Close(), cm.mrMgr.Close())
	cm.server.Stop()
	return err
}

func (cm *ClusterManager) Metadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
}

func (cm *ClusterManager) StorageNodes(ctx context.Context) (ret map[types.StorageNodeID]*snpb.StorageNodeMetadataDescriptor, err error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	metadata, err := cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	var mu sync.Mutex
	ret = make(map[types.StorageNodeID]*snpb.StorageNodeMetadataDescriptor, len(metadata.StorageNodes))
	g, ctx := errgroup.WithContext(ctx)
	for i := range metadata.StorageNodes {
		snd := metadata.StorageNodes[i]
		g.Go(func() error {
			snmd, err := cm.snMgr.GetMetadata(ctx, snd.StorageNodeID)
			if err != nil {
				snmd = &snpb.StorageNodeMetadataDescriptor{
					ClusterID:   cm.clusterID,
					StorageNode: snd.StorageNode,
					Status:      varlogpb.StorageNodeStatusUnavailable,
				}
			}
			mu.Lock()
			defer mu.Unlock()
			ret[snd.StorageNodeID] = snmd
			return nil
		})
	}
	_ = g.Wait()
	return ret, nil
}

func (cm *ClusterManager) MRInfos(ctx context.Context) (*mrpb.ClusterInfo, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.mrMgr.GetClusterInfo(ctx)
}

func (cm *ClusterManager) AddMRPeer(ctx context.Context, raftURL, rpcAddr string) (types.NodeID, error) {
	nodeID := types.NewNodeIDFromURL(raftURL)
	if nodeID == types.InvalidNodeID {
		return nodeID, errors.Wrap(verrors.ErrInvalid, "raft address")
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	err := cm.mrMgr.AddPeer(ctx, nodeID, raftURL, rpcAddr)
	if err != nil {
		if !errors.Is(err, verrors.ErrAlreadyExists) {
			return types.InvalidNodeID, err
		}
	}

	return nodeID, nil
}

func (cm *ClusterManager) RemoveMRPeer(ctx context.Context, raftURL string) error {
	nodeID := types.NewNodeIDFromURL(raftURL)
	if nodeID == types.InvalidNodeID {
		return errors.Wrap(verrors.ErrInvalid, "raft address")
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	err := cm.mrMgr.RemovePeer(ctx, nodeID)
	if err != nil {
		if !errors.Is(err, verrors.ErrAlreadyExists) {
			return err
		}
	}

	return nil
}

// AddStorageNode adds a new storage node to the cluster.
// It is idempotent, that is, adding an already added storage node is okay.
func (cm *ClusterManager) AddStorageNode(ctx context.Context, snid types.StorageNodeID, addr string) (*snpb.StorageNodeMetadataDescriptor, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	snmd, err := getMetadataByAddr(ctx, snid, addr)
	if err != nil {
		return nil, err
	}

	snd := snmd.ToStorageNodeDescriptor()
	snd.Status = varlogpb.StorageNodeStatusRunning
	if err = cm.mrMgr.RegisterStorageNode(ctx, snd); err != nil {
		return nil, err
	}

	cm.snMgr.AddStorageNode(ctx, snmd.StorageNode.StorageNodeID, addr)
	return snmd, err
}

func (cm *ClusterManager) UnregisterStorageNode(ctx context.Context, snid types.StorageNodeID) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	clusmeta, err := cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
	if err != nil {
		return err
	}

	if _, err := clusmeta.MustHaveStorageNode(snid); err != nil {
		return err
	}

	// TODO (jun): Use helper function
	for _, lsdesc := range clusmeta.GetLogStreams() {
		for _, replica := range lsdesc.GetReplicas() {
			if replica.GetStorageNodeID() == snid {
				return errors.New("active log stream")
				// return errors.Wrap(errRunningLogStream, "vms")
			}
		}
	}

	if err := cm.mrMgr.UnregisterStorageNode(ctx, snid); err != nil {
		return err
	}

	cm.snMgr.RemoveStorageNode(snid)
	return nil
}

func (cm *ClusterManager) AddTopic(ctx context.Context) (varlogpb.TopicDescriptor, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	topicID := cm.topicIDGen.Generate()
	if err := cm.mrMgr.RegisterTopic(ctx, topicID); err != nil {
		return varlogpb.TopicDescriptor{}, err
	}

	return varlogpb.TopicDescriptor{TopicID: topicID}, nil
}

func (cm *ClusterManager) Topics(ctx context.Context) ([]varlogpb.TopicDescriptor, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	md, err := cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
	if err != nil || len(md.Topics) == 0 {
		return nil, err
	}

	tds := make([]varlogpb.TopicDescriptor, len(md.Topics))
	for idx := range md.Topics {
		tds[idx] = *md.Topics[idx]
	}
	return tds, nil
}

func (cm *ClusterManager) DescribeTopic(ctx context.Context, tpid types.TopicID) (td varlogpb.TopicDescriptor, lsds []varlogpb.LogStreamDescriptor, err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	md, err := cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
	if err != nil || len(md.Topics) == 0 {
		return
	}

	tdPtr := md.GetTopic(tpid)
	if tdPtr == nil {
		err = errors.Wrapf(verrors.ErrNotExist, "no such topic (topicID=%d)", tpid)
		return
	}
	td = *proto.Clone(tdPtr).(*varlogpb.TopicDescriptor)
	lsds = make([]varlogpb.LogStreamDescriptor, 0, len(td.LogStreams))
	for _, lsID := range td.LogStreams {
		lsdPtr := md.GetLogStream(lsID)
		if lsdPtr == nil {
			continue
		}
		lsd := *proto.Clone(lsdPtr).(*varlogpb.LogStreamDescriptor)
		lsds = append(lsds, lsd)
	}

	return td, lsds, nil
}

func (cm *ClusterManager) UnregisterTopic(ctx context.Context, tpid types.TopicID) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	clusmeta, err := cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
	if err != nil {
		return err
	}

	topicdesc, err := clusmeta.MustHaveTopic(tpid)
	if err != nil {
		return err
	}

	status := topicdesc.GetStatus()
	if status.Deleted() {
		return errors.Errorf("invalid topic status: %s", status)
	}

	//TODO:: seal logStreams and refresh metadata

	return cm.mrMgr.UnregisterTopic(ctx, tpid)
}

func (cm *ClusterManager) AddLogStream(ctx context.Context, tpid types.TopicID, replicas []*varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	lsdesc, err := cm.addLogStreamInternal(ctx, tpid, replicas)
	if err != nil {
		return lsdesc, err
	}

	err = cm.waitSealed(ctx, lsdesc.LogStreamID)
	if err != nil {
		return lsdesc, err
	}

	return cm.Unseal(ctx, tpid, lsdesc.LogStreamID)
}

func (cm *ClusterManager) addLogStreamInternal(ctx context.Context, tpid types.TopicID, replicas []*varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	var err error

	if len(replicas) == 0 {
		replicas, err = cm.snSelector.Select(ctx)
		if err != nil {
			return nil, err
		}
	}

	clusmeta, err := cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}

	// See https://github.daumkakao.com/varlog/varlog/pull/198#discussion_r215602
	logStreamID := cm.logStreamIDGen.Generate()
	if err := clusmeta.MustNotHaveLogStream(logStreamID); err != nil {
		if e := cm.logStreamIDGen.Refresh(ctx); e != nil {
			err = multierr.Append(err, e)
			cm.logger.Panic("could not refresh LogStreamIDGenerator", zap.Error(err))
		}
		return nil, err
	}

	logStreamDesc := &varlogpb.LogStreamDescriptor{
		TopicID:     tpid,
		LogStreamID: logStreamID,
		Status:      varlogpb.LogStreamStatusSealing,
		Replicas:    replicas,
	}

	if err := cm.verifyLogStream(clusmeta, logStreamDesc); err != nil {
		return nil, err
	}

	// TODO: Choose the primary - e.g., shuffle logStreamReplicaMetas
	return cm.addLogStream(ctx, logStreamDesc)
}

func (cm *ClusterManager) waitSealed(ctx context.Context, lsid types.LogStreamID) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			lsStat := cm.statRepository.GetLogStream(lsid).Copy()
			if lsStat.status == varlogpb.LogStreamStatusSealed {
				return nil
			}
		}
	}
}

func (cm *ClusterManager) UnregisterLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.lockLogStreamStatus(lsid)
	defer cm.unlockLogStreamStatus(lsid)

	clusmeta, err := cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
	if err != nil {
		return err
	}

	lsdesc, err := clusmeta.MustHaveLogStream(lsid)
	if err != nil {
		return err
	}

	status := lsdesc.GetStatus()
	// TODO (jun): Check whether status.Deleted means unregistered.
	// If so, is status.Deleted okay or not?
	if status.Running() || status.Deleted() {
		return errors.Errorf("invalid log stream status: %s", status)
	}

	// TODO (jun): test if the log stream has no logs

	return cm.mrMgr.UnregisterLogStream(ctx, lsid)
}

func (cm *ClusterManager) verifyLogStream(clusmeta *varlogpb.MetadataDescriptor, lsdesc *varlogpb.LogStreamDescriptor) error {
	replicas := lsdesc.GetReplicas()
	// the number of logstream replica
	if uint(len(replicas)) != cm.replicationFactor {
		return errors.Errorf("invalid number of log stream replicas: %d", len(replicas))
	}
	// storagenode existence
	for _, replica := range replicas {
		if _, err := clusmeta.MustHaveStorageNode(replica.GetStorageNodeID()); err != nil {
			return err
		}
	}
	// logstream existence
	return clusmeta.MustNotHaveLogStream(lsdesc.GetLogStreamID())
}

func (cm *ClusterManager) addLogStream(ctx context.Context, lsdesc *varlogpb.LogStreamDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	if err := cm.snMgr.AddLogStream(ctx, lsdesc); err != nil {
		return nil, err
	}

	// NB: RegisterLogStream returns nil if the logstream already exists.
	return lsdesc, cm.mrMgr.RegisterLogStream(ctx, lsdesc)
}

func (cm *ClusterManager) RemoveLogStreamReplica(ctx context.Context, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.lockLogStreamStatus(lsid)
	defer cm.unlockLogStreamStatus(lsid)

	clusmeta, err := cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
	if err != nil {
		return err
	}

	if err := cm.removableLogStreamReplica(clusmeta, snid, lsid); err != nil {
		return err
	}

	return cm.snMgr.RemoveLogStreamReplica(ctx, snid, tpid, lsid)
}

func (cm *ClusterManager) UpdateLogStream(ctx context.Context, lsid types.LogStreamID, poppedReplica, pushedReplica *varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	// NOTE (jun): Name of the method - UpdateLogStream can be confused.
	// UpdateLogStream can change only replicas. To update status, use Seal or Unseal.
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.lockLogStreamStatus(lsid)
	defer cm.unlockLogStreamStatus(lsid)

	clusmeta, err := cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}

	oldLSDesc, err := clusmeta.MustHaveLogStream(lsid)
	if err != nil {
		return nil, err
	}

	status := oldLSDesc.GetStatus()
	if status.Running() || status.Deleted() {
		return nil, errors.Errorf("invalid log stream status: %s", status)
	}

	if poppedReplica == nil {
		// TODO: Choose laggy replica
		selector := newVictimSelector(cm.snMgr, lsid, oldLSDesc.GetReplicas())
		victims, err := selector.Select(ctx)
		if err != nil {
			return nil, err
		}
		poppedReplica = victims[0]
	}

	if pushedReplica == nil {
		oldReplicas := oldLSDesc.GetReplicas()
		denylist := make([]types.StorageNodeID, len(oldReplicas))
		for i, replica := range oldReplicas {
			denylist[i] = replica.GetStorageNodeID()
		}

		selector, err := newRandomReplicaSelector(cm.mrMgr.ClusterMetadataView(), 1, denylist...)
		if err != nil {
			return nil, err
		}
		candidates, err := selector.Select(ctx)
		if err != nil {
			return nil, err
		}
		pushedReplica = candidates[0]
	}

	replace := false
	newLSDesc := proto.Clone(oldLSDesc).(*varlogpb.LogStreamDescriptor)
	for i := range newLSDesc.Replicas {
		// TODO - fix? poppedReplica can ignore path.
		if newLSDesc.Replicas[i].GetStorageNodeID() == poppedReplica.GetStorageNodeID() {
			newLSDesc.Replicas[i] = pushedReplica
			replace = true
			break
		}
	}
	if !replace {
		cm.logger.Panic("logstream push/pop error")
	}

	if err := cm.snMgr.AddLogStreamReplica(ctx, pushedReplica.GetStorageNodeID(), newLSDesc.TopicID, lsid, pushedReplica.GetPath()); err != nil {
		return nil, err
	}

	// To reset the status of the log stream, set it as LogStreamStatusRunning
	defer func() {
		cm.statRepository.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning)
	}()

	if err := cm.mrMgr.UpdateLogStream(ctx, newLSDesc); err != nil {
		return nil, err
	}

	return newLSDesc, nil
}

func (cm *ClusterManager) removableLogStreamReplica(clusmeta *varlogpb.MetadataDescriptor, snid types.StorageNodeID, lsid types.LogStreamID) error {
	lsdesc := clusmeta.GetLogStream(lsid)
	if lsdesc == nil {
		// unregistered LS or garbage
		return nil
	}

	replicas := lsdesc.GetReplicas()
	for _, replica := range replicas {
		if replica.GetStorageNodeID() == snid {
			return errors.Wrap(verrors.ErrState, "running log stream is not removable")
		}
	}
	return nil
}

func (cm *ClusterManager) lockLogStreamStatus(lsid types.LogStreamID) {
	cm.muLogStreamStatus[lsid%numLogStreamMutex].Lock()
}

func (cm *ClusterManager) unlockLogStreamStatus(lsid types.LogStreamID) {
	cm.muLogStreamStatus[lsid%numLogStreamMutex].Unlock()
}

// Seal seals the log stream replicas corresponded with the given logStreamID.
func (cm *ClusterManager) Seal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) ([]snpb.LogStreamReplicaMetadataDescriptor, types.GLSN, error) {
	cm.lockLogStreamStatus(lsid)
	defer cm.unlockLogStreamStatus(lsid)

	return cm.seal(ctx, tpid, lsid)
}

func (cm *ClusterManager) seal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) ([]snpb.LogStreamReplicaMetadataDescriptor, types.GLSN, error) {
	cm.statRepository.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusSealing)

	lastGLSN, err := cm.mrMgr.Seal(ctx, lsid)
	if err != nil {
		cm.statRepository.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning)
		return nil, types.InvalidGLSN, err
	}

	result, err := cm.snMgr.Seal(ctx, tpid, lsid, lastGLSN)
	if err != nil {
		cm.statRepository.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning)
	}

	return result, lastGLSN, err
}

// Sync copies the log entries of the src to the dst. Sync may be long-running, thus it
// returns immediately without waiting for the completion of sync. Callers of Sync
// periodically can call Sync, and get the current state of the sync progress.
// SyncState is one of SyncStateError, SyncStateInProgress, or SyncStateComplete. If Sync
// returns SyncStateComplete, all the log entries were copied well. If it returns
// SyncStateInProgress, it is still progressing. Otherwise, if it returns SyncStateError,
// it is stopped by an error.
// To start sync, the log stream status of the src must be LogStreamStatusSealed and the log
// stream status of the dst must be LogStreamStatusSealing. If either of the statuses is not
// correct, Sync returns ErrSyncInvalidStatus.
func (cm *ClusterManager) Sync(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, srcID, dstID types.StorageNodeID) (*snpb.SyncStatus, error) {
	cm.lockLogStreamStatus(lsid)
	defer cm.unlockLogStreamStatus(lsid)

	return cm.sync(ctx, tpid, lsid, srcID, dstID)
}

func (cm *ClusterManager) sync(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, srcID, dstID types.StorageNodeID) (*snpb.SyncStatus, error) {
	lastGLSN, err := cm.mrMgr.Seal(ctx, lsid)
	if err != nil {
		return nil, err
	}
	return cm.snMgr.Sync(ctx, tpid, lsid, srcID, dstID, lastGLSN)
}

// Unseal unseals the log stream replicas corresponded with the given logStreamID.
func (cm *ClusterManager) Unseal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (*varlogpb.LogStreamDescriptor, error) {
	cm.lockLogStreamStatus(lsid)
	defer cm.unlockLogStreamStatus(lsid)

	return cm.unseal(ctx, tpid, lsid)
}

func (cm *ClusterManager) unseal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (*varlogpb.LogStreamDescriptor, error) {
	var err error
	var clusmeta *varlogpb.MetadataDescriptor
	cm.statRepository.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusUnsealing)

	if err = cm.snMgr.Unseal(ctx, tpid, lsid); err != nil {
		goto errOut
	}

	if err = cm.mrMgr.Unseal(ctx, lsid); err != nil {
		goto errOut
	}

	if clusmeta, err = cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx); err != nil {
		goto errOut
	}

	return clusmeta.GetLogStream(lsid), nil

errOut:
	cm.statRepository.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning)
	return nil, err
}

func (cm *ClusterManager) HandleHeartbeatTimeout(ctx context.Context, snid types.StorageNodeID) {
	meta, err := cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
	if err != nil {
		return
	}

	//TODO: store sn status
	for _, ls := range meta.GetLogStreams() {
		if ls.IsReplica(snid) {
			cm.logger.Debug("seal due to heartbeat timeout", zap.Any("snid", snid), zap.Any("lsid", ls.LogStreamID))
			cm.Seal(ctx, ls.TopicID, ls.LogStreamID)
		}
	}
}

func (cm *ClusterManager) checkLogStreamStatus(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, mrStatus, replicaStatus varlogpb.LogStreamStatus) {
	cm.lockLogStreamStatus(lsid)
	defer cm.unlockLogStreamStatus(lsid)

	lsStat := cm.statRepository.GetLogStream(lsid).Copy()

	switch lsStat.Status() {
	case varlogpb.LogStreamStatusRunning:
		if mrStatus.Sealed() || replicaStatus.Sealed() {
			cm.logger.Info("seal due to status mismatch", zap.Any("lsid", lsid))
			cm.seal(ctx, tpid, lsid)
		}

	case varlogpb.LogStreamStatusSealing:
		for _, r := range lsStat.Replicas() {
			if r.Status != varlogpb.LogStreamStatusSealed {
				cm.logger.Info("seal due to status", zap.Any("lsid", lsid))
				cm.seal(ctx, tpid, lsid)
				return
			}
		}
		cm.logger.Info("sealed", zap.Any("lsid", lsid))
		cm.statRepository.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusSealed)

	case varlogpb.LogStreamStatusSealed:
		for _, r := range lsStat.Replicas() {
			if r.Status != varlogpb.LogStreamStatusSealed {
				cm.statRepository.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusSealing)
				return
			}
		}

	case varlogpb.LogStreamStatusUnsealing:
		for _, r := range lsStat.Replicas() {
			if r.Status == varlogpb.LogStreamStatusRunning {
				continue
			} else if r.Status == varlogpb.LogStreamStatusSealed {
				return
			} else if r.Status == varlogpb.LogStreamStatusSealing {
				cm.logger.Info("seal due to unexpected status", zap.Any("lsid", lsid))
				cm.seal(ctx, tpid, lsid)
				return
			}
		}
		cm.statRepository.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning)
	}
}

func (cm *ClusterManager) syncLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) {
	cm.lockLogStreamStatus(logStreamID)
	defer cm.unlockLogStreamStatus(logStreamID)

	min, max := types.MaxVersion, types.InvalidVersion
	var src, tgt types.StorageNodeID

	lsStat := cm.statRepository.GetLogStream(logStreamID).Copy()

	if !lsStat.Status().Sealed() {
		return
	}

	snIDs := make([]types.StorageNodeID, 0, len(lsStat.Replicas()))
	for snID := range lsStat.Replicas() {
		snIDs = append(snIDs, snID)
	}
	sort.Slice(snIDs, func(i, j int) bool { return snIDs[i] < snIDs[j] })

	for i, snID := range snIDs {
		r, _ := lsStat.Replica(snID)

		if !r.Status.Sealed() {
			return
		}

		if r.Status == varlogpb.LogStreamStatusSealing && (i == 0 || r.Version < min) {
			min = r.Version
			tgt = snID
		}

		if r.Status == varlogpb.LogStreamStatusSealed && (i == 0 || r.Version > max) {
			max = r.Version
			src = snID
		}
	}

	// FIXME (jun): Since there is no invalid identifier for the storage
	// node, it cannot check whether a source or target is selected. It
	// thus checks that min and max are in a valid range.
	if src != tgt && !max.Invalid() && min != types.MaxVersion {
		status, err := cm.sync(ctx, topicID, logStreamID, src, tgt)
		cm.logger.Debug("sync", zap.Any("lsid", logStreamID), zap.Any("src", src), zap.Any("dst", tgt), zap.String("status", status.String()), zap.Error(err))

		//TODO: Unseal
		//status, _ := cm.Sync(context.TODO(), ls.LogStreamID, src, tgt)
		//if status.GetState() == snpb.SyncStateComplete {
		//cm.Unseal(context.TODO(), ls.LogStreamID)
		//}
	}
}

func (cm *ClusterManager) HandleReport(ctx context.Context, snm *snpb.StorageNodeMetadataDescriptor, gcTimeout time.Duration) {
	meta, err := cm.mrMgr.ClusterMetadataView().ClusterMetadata(ctx)
	if err != nil {
		return
	}

	cm.statRepository.Report(ctx, snm)

	// Sync LogStreamStatus
	for _, ls := range snm.GetLogStreamReplicas() {
		mls := meta.GetLogStream(ls.LogStreamID)
		if mls != nil {
			cm.checkLogStreamStatus(ctx, ls.TopicID, ls.LogStreamID, mls.Status, ls.Status)
			continue
		}
		if time.Since(ls.CreatedTime) > gcTimeout {
			cm.RemoveLogStreamReplica(ctx, snm.StorageNode.StorageNodeID, ls.TopicID, ls.LogStreamID)
		}
	}

	// Sync LogStream
	for _, ls := range snm.GetLogStreamReplicas() {
		if ls.Status.Sealed() {
			cm.syncLogStream(ctx, ls.TopicID, ls.LogStreamID)
		}
	}
}

func (cm *ClusterManager) Trim(ctx context.Context, tpid types.TopicID, lastGLSN types.GLSN) ([]vmspb.TrimResult, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.snMgr.Trim(ctx, tpid, lastGLSN)
}

func getMetadataByAddr(ctx context.Context, snid types.StorageNodeID, addr string) (*snpb.StorageNodeMetadataDescriptor, error) {
	mgr, err := client.NewManager[*client.ManagementClient]()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = mgr.Close()
	}()

	mc, err := mgr.GetOrConnect(ctx, snid, addr)
	if err != nil {
		return nil, err
	}

	return mc.GetMetadata(ctx)
}
