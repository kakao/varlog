package vms

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/netutil"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/runner/stopwaiter"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ClusterManager manages varlog cluster.
type ClusterManager interface {
	// AddStorageNode adds new StorageNode to the cluster.
	AddStorageNode(ctx context.Context, addr string) (*vpb.StorageNodeMetadataDescriptor, error)

	AddLogStream(ctx context.Context) (*vpb.LogStreamDescriptor, error)

	// AddLogStream adds new LogStream to the cluster.
	AddLogStreamWith(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) (*vpb.LogStreamDescriptor, error)

	// Seal seals the log stream replicas corresponded with the given logStreamID.
	Seal(ctx context.Context, logStreamID types.LogStreamID) error

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
	logStreamIDGen LogStreamIDGenerator

	logger  *zap.Logger
	options *Options
}

func NewClusterManager(opts *Options) (ClusterManager, error) {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	opts.Logger = opts.Logger.Named("vms")

	mrMgr, err := NewMRManager(opts.ClusterID, opts.MetadataRepositoryAddresses)
	if err != nil {
		return nil, err
	}
	cmView := NewClusterMetadataView(mrMgr, opts.Logger)
	snMgr := NewStorageNodeManager(cmView, opts.Logger)
	cm := &clusterManager{
		sw:             stopwaiter.New(),
		cmState:        clusterManagerReady,
		snMgr:          snMgr,
		mrMgr:          mrMgr,
		cmView:         cmView,
		snSelector:     NewRandomSNSelector(),
		logStreamIDGen: NewLogStreamIDGenerator(),
		logger:         opts.Logger,
		options:        opts,
	}
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

	cm.server.Stop()
	cm.sw.Stop()
	cm.logger.Info("stop")
}

func (cm *clusterManager) Metadata(ctx context.Context) (*vpb.MetadataDescriptor, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.cmView.ClusterMetadata(ctx)
}

// FIXME: use varlog errors
func (cm *clusterManager) AddStorageNode(ctx context.Context, addr string) (*vpb.StorageNodeMetadataDescriptor, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.snMgr.FindByAddress(addr) != nil {
		return nil, varlog.ErrVMSStorageNodeExisted
	}

	snmcl, snmeta, err := cm.snMgr.GetMetadataByAddr(ctx, addr)
	if err != nil {
		return nil, err
	}
	storageNodeID := snmcl.PeerStorageNodeID()
	if cm.snMgr.FindByStorageNodeID(storageNodeID) != nil {
		return nil, varlog.ErrVMSDuplicatedStorageNodeID
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

func (cm *clusterManager) AddLogStream(ctx context.Context) (*vpb.LogStreamDescriptor, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cmeta, err := cm.cmView.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	snDescList, err := cm.snSelector.SelectStorageNode(cmeta, cm.options.ReplicationFactor)
	if err != nil {
		return nil, err
	}
	logStreamID := cm.logStreamIDGen.Generate()
	logStreamDesc := &vpb.LogStreamDescriptor{
		LogStreamID: logStreamID,
		Status:      vpb.LogStreamStatusRunning,
		Replicas:    make([]*vpb.ReplicaDescriptor, cm.options.ReplicationFactor),
	}
	for idx, snDesc := range snDescList {
		logStreamDesc.Replicas[idx] = &vpb.ReplicaDescriptor{
			StorageNodeID: snDesc.GetStorageNodeID(),
			// TODO: snSelector can be expanded to choose path
			Path: snDesc.GetStorages()[0].Path,
		}
	}
	// TODO: Choose the primary - e.g., shuffle logStreamReplicaMetas
	return cm.AddLogStreamWith(ctx, logStreamDesc)
}

func (cm *clusterManager) AddLogStreamWith(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) (*vpb.LogStreamDescriptor, error) {
	if err := cm.snMgr.AddLogStream(ctx, logStreamDesc); err != nil {
		return nil, err
	}
	err := cm.mrMgr.RegisterLogStream(ctx, logStreamDesc)
	return logStreamDesc, err
}

func (cm *clusterManager) Seal(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
	/*
		lastGLSN, err := cm.mrMgr.Seal(ctx, logStreamID)
		if err != nil {
			return err
		}
		return cm.snMgr.Seal(ctx, logStreamID, lastGLSN)
	*/
}

func (cm *clusterManager) Sync(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
	// return cm.snMgr.Sync(ctx, logStreamID)
}

func (cm *clusterManager) Unseal(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
	// return cm.snMgr.Unseal(ctx, logStreamID)
}
