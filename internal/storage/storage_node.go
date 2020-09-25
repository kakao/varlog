package storage

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/netutil"
	"github.com/kakao/varlog/pkg/varlog/util/runner/stopwaiter"
	snpb "github.com/kakao/varlog/proto/storage_node"
	vpb "github.com/kakao/varlog/proto/varlog"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Management is the interface that wraps methods for managing StorageNode.
type Management interface {
	// GetMetadata returns metadata of StorageNode. The metadata contains
	// configurations and statistics for StorageNode.
	GetMetadata(clusterID types.ClusterID, metadataType snpb.MetadataType) (*vpb.StorageNodeMetadataDescriptor, error)

	// AddLogStream adds a new LogStream to StorageNode.
	AddLogStream(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, path string) (string, error)

	// RemoveLogStream removes a LogStream from StorageNode.
	RemoveLogStream(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error

	// Seal changes status of LogStreamExecutor corresponding to the
	// LogStreamID to LogStreamStatusSealing or LogStreamStatusSealed.
	Seal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (vpb.LogStreamStatus, types.GLSN, error)

	// Unseal changes status of LogStreamExecutor corresponding to the
	// LogStreamID to LogStreamStatusRunning.
	Unseal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error

	Sync(ctx context.Context, clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, replica Replica, lastGLSN types.GLSN) (snpb.SyncState, error)
}

type LogStreamExecutorGetter interface {
	GetLogStreamExecutor(logStreamID types.LogStreamID) (LogStreamExecutor, bool)
	GetLogStreamExecutors() []LogStreamExecutor
}

type StorageNode struct {
	clusterID     types.ClusterID
	storageNodeID types.StorageNodeID

	server     *grpc.Server
	serverAddr string

	running   bool
	muRunning sync.Mutex
	sw        *stopwaiter.StopWaiter

	lseMtx sync.RWMutex
	lseMap map[types.LogStreamID]LogStreamExecutor

	lsr LogStreamReporter

	options *StorageNodeOptions
	logger  *zap.Logger
}

func NewStorageNode(options *StorageNodeOptions) (*StorageNode, error) {
	sn := &StorageNode{
		clusterID:     options.ClusterID,
		storageNodeID: options.StorageNodeID,
		lseMap:        make(map[types.LogStreamID]LogStreamExecutor),
		options:       options,
		logger:        options.Logger,
		sw:            stopwaiter.New(),
	}
	if sn.logger == nil {
		sn.logger = zap.NewNop()
	}
	sn.logger = sn.logger.Named(fmt.Sprintf("storagenode")).With(zap.Any("cid", sn.clusterID), zap.Any("snid", sn.storageNodeID))
	sn.lsr = NewLogStreamReporter(sn.logger, sn.storageNodeID, sn, &options.LogStreamReporterOptions)
	sn.server = grpc.NewServer()

	NewLogStreamReporterService(sn.lsr, sn.logger).Register(sn.server)
	NewLogIOService(sn.storageNodeID, sn, sn.logger).Register(sn.server)
	NewManagementService(sn, sn.logger).Register(sn.server)
	NewReplicatorService(sn.storageNodeID, sn, sn.logger).Register(sn.server)

	return sn, nil
}

func (sn *StorageNode) Run() error {
	sn.muRunning.Lock()
	defer sn.muRunning.Unlock()

	if sn.running {
		return nil
	}
	sn.running = true

	// LogStreamReporter
	if err := sn.lsr.Run(context.Background()); err != nil {
		return err
	}

	// Listener
	lis, err := net.Listen("tcp", sn.options.RPCBindAddress)
	if err != nil {
		sn.logger.Error("could not listen", zap.Error(err))
		return err
	}
	addrs, _ := netutil.GetListenerAddrs(lis.Addr())
	// TODO (jun): choose best address
	sn.serverAddr = addrs[0]

	// RPC Server
	go func() {
		if err := sn.server.Serve(lis); err != nil {
			sn.logger.Error("could not serve", zap.Error(err))
			sn.Close()
		}
	}()

	sn.logger.Info("start")
	return nil
}

func (sn *StorageNode) Close() {
	sn.muRunning.Lock()
	defer sn.muRunning.Unlock()

	if !sn.running {
		return
	}
	sn.running = false

	// LogStreamReporter
	sn.lsr.Close()

	// LogStreamExecutors
	for _, lse := range sn.GetLogStreamExecutors() {
		lse.Close()
	}

	// RPC Server
	// FIXME (jun): Use GracefulStop
	sn.server.Stop()

	sn.sw.Stop()
	sn.logger.Info("stop")
}

func (sn *StorageNode) Wait() {
	sn.sw.Wait()
}

// GetMeGetMetadata implements the Management GetMetadata method.
func (sn *StorageNode) GetMetadata(cid types.ClusterID, metadataType snpb.MetadataType) (*vpb.StorageNodeMetadataDescriptor, error) {
	if !sn.verifyClusterID(cid) {
		return nil, varlog.ErrInvalidArgument
	}

	ret := &vpb.StorageNodeMetadataDescriptor{
		ClusterID: sn.clusterID,
		StorageNode: &vpb.StorageNodeDescriptor{
			StorageNodeID: sn.storageNodeID,
			Address:       sn.serverAddr,
			Status:        vpb.StorageNodeStatusRunning, // TODO (jun), Ready, Running, Stopping
		},
	}
	if metadataType == snpb.MetadataTypeHeartbeat {
		return ret, nil
	}

	ret.LogStreams = sn.logStreamDescriptors()
	return ret, nil

	// TODO (jun): add statistics to the response of GetMetadata
}

func (sn *StorageNode) logStreamDescriptors() []vpb.LogStreamDescriptor {
	lseList := sn.GetLogStreamExecutors()
	if len(lseList) == 0 {
		return nil
	}
	lsdList := make([]vpb.LogStreamDescriptor, len(lseList))
	for i, lse := range lseList {
		lse.LogStreamID()
		lsdList[i] = vpb.LogStreamDescriptor{
			LogStreamID: lse.LogStreamID(),
			Status:      lse.Status(),
			Replicas: []*vpb.ReplicaDescriptor{
				{
					StorageNodeID: sn.storageNodeID,
					// TODO (jun): path represents disk-based storage and
					// memory-based storage
					// Path: lse.Path(),
				},
			},
		}
	}
	return lsdList
}

// AddLogStream implements the Management AddLogStream method.
func (sn *StorageNode) AddLogStream(cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID, path string) (string, error) {
	if !sn.verifyClusterID(cid) || !sn.verifyStorageNodeID(snid) {
		return "", varlog.ErrInvalidArgument
	}
	sn.lseMtx.Lock()
	defer sn.lseMtx.Unlock()
	_, ok := sn.lseMap[lsid]
	if ok {
		return "", varlog.ErrExist // FIXME: ErrExist or ErrAlreadyExists
	}
	// TODO(jun): Create Storage and add new LSE
	var stg Storage = NewInMemoryStorage()
	var stgPath string
	lse, err := NewLogStreamExecutor(sn.logger, lsid, stg, &sn.options.LogStreamExecutorOptions)
	if err != nil {
		return "", err
	}

	if err := lse.Run(context.Background()); err != nil {
		lse.Close()
		return "", err
	}

	sn.lseMap[lsid] = lse
	return stgPath, nil
}

// RemoveLogStream implements the Management RemoveLogStream method.
func (sn *StorageNode) RemoveLogStream(cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID) error {
	if !sn.verifyClusterID(cid) || !sn.verifyStorageNodeID(snid) {
		return varlog.ErrInvalidArgument
	}
	sn.lseMtx.Lock()
	defer sn.lseMtx.Unlock()
	_, ok := sn.lseMap[lsid]
	if !ok {
		return varlog.ErrNotExist
	}
	delete(sn.lseMap, lsid)
	return nil
}

// Seal implements the Management Seal method.
func (sn *StorageNode) Seal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (vpb.LogStreamStatus, types.GLSN, error) {
	if !sn.verifyClusterID(clusterID) || !sn.verifyStorageNodeID(storageNodeID) {
		return vpb.LogStreamStatusRunning, types.InvalidGLSN, varlog.ErrInvalidArgument
	}
	lse, ok := sn.GetLogStreamExecutor(logStreamID)
	if !ok {
		return vpb.LogStreamStatusRunning, types.InvalidGLSN, varlog.ErrInvalidArgument
	}
	status, hwm := lse.Seal(lastCommittedGLSN)
	return status, hwm, nil
}

// Unseal implements the Management Unseal method.
func (sn *StorageNode) Unseal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error {
	if !sn.verifyClusterID(clusterID) || !sn.verifyStorageNodeID(storageNodeID) {
		return varlog.ErrInvalidArgument
	}
	lse, ok := sn.GetLogStreamExecutor(logStreamID)
	if !ok {
		return varlog.ErrInvalidArgument
	}
	return lse.Unseal()
}

func (sn *StorageNode) Sync(ctx context.Context, clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, replica Replica, lastGLSN types.GLSN) (snpb.SyncState, error) {
	if !sn.verifyClusterID(clusterID) || !sn.verifyStorageNodeID(storageNodeID) {
		return snpb.SyncStateError, varlog.ErrInvalidArgument
	}
	lse, ok := sn.GetLogStreamExecutor(logStreamID)
	if !ok {
		sn.logger.Error("could not get LSE")
		return snpb.SyncStateError, varlog.ErrInvalidArgument
	}
	sts, err := lse.Sync(ctx, replica, lastGLSN)
	if err != nil {
		return snpb.SyncStateError, err
	}
	return sts.State, err
}

func (sn *StorageNode) GetLogStreamExecutor(logStreamID types.LogStreamID) (LogStreamExecutor, bool) {
	sn.lseMtx.RLock()
	defer sn.lseMtx.RUnlock()
	lse, ok := sn.lseMap[logStreamID]
	return lse, ok
}

func (sn *StorageNode) GetLogStreamExecutors() []LogStreamExecutor {
	sn.lseMtx.RLock()
	ret := make([]LogStreamExecutor, 0, len(sn.lseMap))
	for _, lse := range sn.lseMap {
		ret = append(ret, lse)
	}
	sn.lseMtx.RUnlock()
	return ret
}

func (sn *StorageNode) verifyClusterID(cid types.ClusterID) bool {
	return sn.clusterID == cid
}

func (sn *StorageNode) verifyStorageNodeID(snid types.StorageNodeID) bool {
	return sn.storageNodeID == snid
}
