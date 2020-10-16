package vms

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/netutil"
	"github.com/kakao/varlog/pkg/varlog/util/runner/stopwaiter"
	vpb "github.com/kakao/varlog/proto/varlog"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type StorageNodeEventHandler interface {
	HeartbeatTimeout(types.StorageNodeID)

	Report(*vpb.StorageNodeMetadataDescriptor)
}

// ClusterManager manages varlog cluster.
type ClusterManager interface {
	// AddStorageNode adds new StorageNode to the cluster.
	AddStorageNode(ctx context.Context, addr string) (*vpb.StorageNodeMetadataDescriptor, error)

	AddLogStream(ctx context.Context, replicas []*vpb.ReplicaDescriptor) (*vpb.LogStreamDescriptor, error)

	// Seal seals the log stream replicas corresponded with the given logStreamID.
	Seal(ctx context.Context, logStreamID types.LogStreamID) ([]vpb.LogStreamMetadataDescriptor, error)

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
	Sync(ctx context.Context, logStreamID types.LogStreamID) error

	// Unseal unseals the log stream replicas corresponded with the given logStreamID.
	Unseal(ctx context.Context, logStreamID types.LogStreamID) error

	Metadata(ctx context.Context) (*vpb.MetadataDescriptor, error)

	Run() error

	Address() string

	Close()

	Wait()
}

var _ ClusterManager = (*clusterManager)(nil)

var errCMKilled = errors.New("killed cluster manager")

type clusterManagerState int

const (
	clusterManagerReady clusterManagerState = iota
	clusterManagerRunning
	clusterManagerClosed
)

type clusterManager struct {
	server     *grpc.Server
	serverAddr string

	// single large lock
	mu sync.RWMutex

	cmState clusterManagerState
	sw      *stopwaiter.StopWaiter

	snMgr          StorageNodeManager
	mrMgr          MetadataRepositoryManager
	cmView         ClusterMetadataView
	snSelector     StorageNodeSelector
	snWatcher      StorageNodeWatcher
	logStreamIDGen LogStreamIDGenerator

	logger  *zap.Logger
	options *Options
}

func NewClusterManager(ctx context.Context, opts *Options) (ClusterManager, error) {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	opts.Logger = opts.Logger.Named("vms").With(zap.Any("cid", opts.ClusterID))

	mrMgr, err := NewMRManager(opts.ClusterID, opts.MetadataRepositoryAddresses, opts.Logger)
	if err != nil {
		return nil, err
	}

	cmView := mrMgr.ClusterMetadataView()

	snMgr := NewStorageNodeManager(cmView, opts.Logger)
	if err := snMgr.Init(); err != nil {
		return nil, err
	}

	logStreamIDGen, err := NewSequentialLogStreamIDGenerator(ctx, cmView, snMgr)
	if err != nil {
		return nil, err
	}

	cm := &clusterManager{
		sw:             stopwaiter.New(),
		cmState:        clusterManagerReady,
		snMgr:          snMgr,
		mrMgr:          mrMgr,
		cmView:         cmView,
		snSelector:     NewRandomSNSelector(cmView),
		logStreamIDGen: logStreamIDGen,
		logger:         opts.Logger,
		options:        opts,
	}

	cm.snWatcher = NewStorageNodeWatcher(opts.WatcherOptions, cmView, snMgr, cm, opts.Logger)

	cm.server = grpc.NewServer()

	NewClusterManagerService(cm, cm.logger).Register(cm.server)

	return cm, nil
}

func (cm *clusterManager) Address() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.serverAddr
}

func (cm *clusterManager) Run() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	switch cm.cmState {
	case clusterManagerRunning:
		return nil
	case clusterManagerClosed:
		return errCMKilled
	}
	cm.cmState = clusterManagerRunning

	// Listener
	lis, err := net.Listen("tcp", cm.options.RPCBindAddress)
	if err != nil {
		cm.logger.Error("could not listen", zap.Error(err))
		return err
	}
	addrs, _ := netutil.GetListenerAddrs(lis.Addr())
	// TODO (jun): choose best address
	cm.serverAddr = addrs[0]

	// RPC Server
	go func() {
		if err := cm.server.Serve(lis); err != nil {
			cm.logger.Error("could not serve", zap.Error(err))
			cm.Close()
		}
	}()

	// SN Watcher
	cm.snWatcher.Run()

	cm.logger.Info("start")
	return nil
}

func (cm *clusterManager) Wait() {
	cm.sw.Wait()
}

func (cm *clusterManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	switch cm.cmState {
	case clusterManagerReady:
		cm.logger.Error("could not close not-running cluster manager")
		return
	case clusterManagerClosed:
		return
	}
	cm.cmState = clusterManagerClosed

	// SN Watcher
	cm.snWatcher.Close()

	cm.server.Stop()
	cm.sw.Stop()
	cm.logger.Info("stop")
}

func (cm *clusterManager) Metadata(ctx context.Context) (*vpb.MetadataDescriptor, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.cmView.ClusterMetadata(ctx)
}

func (cm *clusterManager) AddStorageNode(ctx context.Context, addr string) (*vpb.StorageNodeMetadataDescriptor, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.snMgr.FindByAddress(addr) != nil {
		return nil, varlog.ErrStorageNodeAlreadyExists
	}

	snmcl, snmeta, err := cm.snMgr.GetMetadataByAddr(ctx, addr)
	if err != nil {
		return nil, err
	}
	storageNodeID := snmcl.PeerStorageNodeID()
	if cm.snMgr.FindByStorageNodeID(storageNodeID) != nil {
		return nil, varlog.ErrStorageNodeAlreadyExists
	}

	_, err = cm.cmView.StorageNode(ctx, storageNodeID)
	if err == nil {
		cm.logger.Panic("mismatch between clusterMetadataView and snManager")
	}
	if err != errCMVNoStorageNode {
		goto err_out
	}

	if err = cm.mrMgr.RegisterStorageNode(ctx, snmeta.GetStorageNode()); err != nil {
		goto err_out
	}

	if err = cm.snMgr.AddStorageNode(ctx, snmcl); err == nil {
		return snmeta, nil
	}

err_out:
	snmcl.Close()
	return nil, err
}

func (cm *clusterManager) AddLogStream(ctx context.Context, replicas []*vpb.ReplicaDescriptor) (*vpb.LogStreamDescriptor, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	clusmeta, err := cm.cmView.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}

	if len(replicas) == 0 {
		replicas, err = cm.snSelector.SelectStorageNodeAndPath(ctx, cm.options.ReplicationFactor)
		if err != nil {
			return nil, err
		}
	}

	// See https://github.com/kakao/varlog/pull/198#discussion_r215602
	logStreamID := cm.logStreamIDGen.Generate()
	if clusmeta.GetLogStream(logStreamID) != nil {
		err := varlog.NewErrorf(varlog.ErrLogStreamAlreadyExists, codes.Unavailable, "lsid=%v", logStreamID)
		cm.logger.Error("mismatch between ClusterMetadataView and LogStreamIDGenerator", zap.Any("lsid", logStreamID), zap.Error(err))
		if err := cm.logStreamIDGen.Refresh(ctx); err != nil {
			cm.logger.Panic("could not refresh LogStreamIDGenerator", zap.Error(err))
		}
		return nil, err
	}

	logStreamDesc := &vpb.LogStreamDescriptor{
		LogStreamID: logStreamID,
		Status:      vpb.LogStreamStatusRunning,
		Replicas:    replicas,
	}

	if err := cm.verifyLogStream(clusmeta, logStreamDesc); err != nil {
		return nil, err
	}

	// TODO: Choose the primary - e.g., shuffle logStreamReplicaMetas
	return cm.addLogStream(ctx, logStreamDesc)
}

func (cm *clusterManager) verifyLogStream(clusterMetadata *vpb.MetadataDescriptor, logStreamDesc *vpb.LogStreamDescriptor) error {
	replicas := logStreamDesc.GetReplicas()
	// the number of logstream replica
	if uint(len(replicas)) != cm.options.ReplicationFactor {
		return fmt.Errorf("vms: incorrect number of logstream replicas: %w", varlog.ErrInvalid)
	}
	// storagenode existence
	for _, replica := range replicas {
		if clusterMetadata.GetStorageNode(replica.GetStorageNodeID()) == nil {
			return varlog.ErrStorageNodeNotExist
		}
	}
	// logstream existence
	if clusterMetadata.GetLogStream(logStreamDesc.GetLogStreamID()) != nil {
		return varlog.ErrLogStreamAlreadyExists
	}
	return nil
}

func (cm *clusterManager) addLogStream(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) (*vpb.LogStreamDescriptor, error) {
	if err := cm.snMgr.AddLogStream(ctx, logStreamDesc); err != nil {
		// Unlike verifyLogStream, ErrLogStreamAlreadyExists is transient error in here.
		if errors.Is(err, varlog.ErrLogStreamAlreadyExists) {
			cm.logger.Warn("not registered, duplicated logstream id", zap.Error(err))
			return nil, varlog.NewErrorf(err, codes.Unavailable, "lsid=%v", logStreamDesc.GetLogStreamID())
		}
		return nil, err
	}

	// NB: RegisterLogStream returns nil if the logstream already exists.
	return logStreamDesc, cm.mrMgr.RegisterLogStream(ctx, logStreamDesc)
}

func (cm *clusterManager) Seal(ctx context.Context, logStreamID types.LogStreamID) ([]vpb.LogStreamMetadataDescriptor, error) {
	lastGLSN, err := cm.mrMgr.Seal(ctx, logStreamID)
	if err != nil {
		cm.logger.Error("error while sealing by MR", zap.Error(err))
		return nil, err
	}
	return cm.snMgr.Seal(ctx, logStreamID, lastGLSN)
}

func (cm *clusterManager) Sync(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
	// return cm.snMgr.Sync(ctx, logStreamID)
}

func (cm *clusterManager) Unseal(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
	// return cm.snMgr.Unseal(ctx, logStreamID)
}

func (cm *clusterManager) HeartbeatTimeout(snID types.StorageNodeID) {
	// not implemented
}

func (cm *clusterManager) Report(sn *vpb.StorageNodeMetadataDescriptor) {
	// not implemented
}
