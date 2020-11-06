package storagenode

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/netutil"
	"github.com/kakao/varlog/pkg/util/runner/stopwaiter"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

// Management is the interface that wraps methods for managing StorageNode.
type Management interface {
	// GetMetadata returns metadata of StorageNode. The metadata contains
	// configurations and statistics for StorageNode.
	GetMetadata(clusterID types.ClusterID, metadataType snpb.MetadataType) (*varlogpb.StorageNodeMetadataDescriptor, error)

	// AddLogStream adds a new LogStream to StorageNode.
	AddLogStream(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, storageNodePath string) (string, error)

	// RemoveLogStream removes a LogStream from StorageNode.
	RemoveLogStream(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error

	// Seal changes status of LogStreamExecutor corresponding to the
	// LogStreamID to LogStreamStatusSealing or LogStreamStatusSealed.
	Seal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error)

	// Unseal changes status of LogStreamExecutor corresponding to the
	// LogStreamID to LogStreamStatusRunning.
	Unseal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error

	Sync(ctx context.Context, clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, replica Replica, lastGLSN types.GLSN) (*snpb.SyncStatus, error)
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

	storageNodePaths map[string]struct{}

	options *Options
	tst     Timestamper

	logger *zap.Logger
}

func NewStorageNode(options *Options) (*StorageNode, error) {
	if err := options.Valid(); err != nil {
		return nil, err
	}

	sn := &StorageNode{
		clusterID:        options.ClusterID,
		storageNodeID:    options.StorageNodeID,
		lseMap:           make(map[types.LogStreamID]LogStreamExecutor),
		tst:              NewTimestamper(),
		storageNodePaths: make(map[string]struct{}),
		options:          options,
		logger:           options.Logger,
		sw:               stopwaiter.New(),
	}
	if sn.logger == nil {
		sn.logger = zap.NewNop()
	}

	for volume := range options.Volumes {
		snPath, err := volume.CreateStorageNodePath(sn.clusterID, sn.storageNodeID)
		if err != nil {
			sn.logger.Error("could not create data directory", zap.Any("storage_node_path", snPath), zap.Error(err))
			return nil, err
		}
		sn.storageNodePaths[snPath] = struct{}{}
	}

	if len(sn.storageNodePaths) == 0 {
		sn.logger.Error("no valid storage node path")
		return nil, verrors.ErrInvalid
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
func (sn *StorageNode) GetMetadata(cid types.ClusterID, metadataType snpb.MetadataType) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	if !sn.verifyClusterID(cid) {
		return nil, verrors.ErrInvalidArgument
	}

	snmeta := &varlogpb.StorageNodeMetadataDescriptor{
		ClusterID: sn.clusterID,
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNodeID: sn.storageNodeID,
			Address:       sn.serverAddr,
			Status:        varlogpb.StorageNodeStatusRunning, // TODO (jun), Ready, Running, Stopping,
		},
		CreatedTime: sn.tst.Created(),
		UpdatedTime: sn.tst.LastUpdated(),
	}
	for snPath := range sn.storageNodePaths {
		snmeta.StorageNode.Storages = append(snmeta.StorageNode.Storages, &varlogpb.StorageDescriptor{
			Path:  snPath,
			Used:  0,
			Total: 0,
		})
	}

	/* disable metadata type
	if metadataType == snpb.MetadataTypeHeartbeat {
		return snmeta, nil
	}
	*/

	snmeta.LogStreams = sn.logStreamMetadataDescriptors()
	return snmeta, nil

	// TODO (jun): add statistics to the response of GetMetadata
}

func (sn *StorageNode) logStreamMetadataDescriptors() []varlogpb.LogStreamMetadataDescriptor {
	lseList := sn.GetLogStreamExecutors()
	if len(lseList) == 0 {
		return nil
	}
	lsmetas := make([]varlogpb.LogStreamMetadataDescriptor, len(lseList))
	for i, lse := range lseList {
		lsmetas[i] = varlogpb.LogStreamMetadataDescriptor{
			StorageNodeID: sn.storageNodeID,
			LogStreamID:   lse.LogStreamID(),
			Status:        lse.Status(),
			HighWatermark: lse.HighWatermark(),
			// TODO (jun): path represents disk-based storage and
			// memory-based storage
			Path:        lse.Path(),
			CreatedTime: lse.Created(),
			UpdatedTime: lse.LastUpdated(),
		}
	}
	return lsmetas
}

// AddLogStream implements the Management AddLogStream method.
func (sn *StorageNode) AddLogStream(cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID, storageNodePath string) (string, error) {
	if !sn.verifyClusterID(cid) || !sn.verifyStorageNodeID(snid) {
		return "", verrors.ErrInvalidArgument
	}

	sn.lseMtx.Lock()
	defer sn.lseMtx.Unlock()

	_, ok := sn.lseMap[lsid]
	if ok {
		return "", verrors.ErrLogStreamAlreadyExists
	}

	if _, exists := sn.storageNodePaths[storageNodePath]; !exists {
		sn.logger.Error("no such storage path", zap.String("path", storageNodePath), zap.Reflect("storage_node_paths", sn.storageNodePaths))
		return "", verrors.ErrNotExist
	}

	lsPath, err := CreateLogStreamPath(storageNodePath, lsid)
	if err != nil {
		return "", err
	}

	storage, err := NewStorage(sn.options.StorageName, WithPath(lsPath), WithLogger(sn.logger))
	if err != nil {
		return "", err
	}
	lse, err := NewLogStreamExecutor(sn.logger, lsid, storage, &sn.options.LogStreamExecutorOptions)
	if err != nil {
		return "", err
	}

	if err := lse.Run(context.Background()); err != nil {
		lse.Close()
		return "", err
	}
	sn.tst.Touch()
	sn.lseMap[lsid] = lse
	return lsPath, nil
}

// RemoveLogStream implements the Management RemoveLogStream method.
func (sn *StorageNode) RemoveLogStream(cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID) error {
	if !sn.verifyClusterID(cid) || !sn.verifyStorageNodeID(snid) {
		return verrors.ErrInvalidArgument
	}
	sn.lseMtx.Lock()
	defer sn.lseMtx.Unlock()
	lse, ok := sn.lseMap[lsid]
	if !ok {
		return verrors.ErrNotExist
	}
	delete(sn.lseMap, lsid)
	lse.Close()
	// TODO (jun): Is removing data path optional or default behavior?
	if err := os.RemoveAll(lse.Path()); err != nil {
		sn.logger.Warn("error while removing log stream path")
	}
	sn.tst.Touch()
	return nil
}

// Seal implements the Management Seal method.
func (sn *StorageNode) Seal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	if !sn.verifyClusterID(clusterID) || !sn.verifyStorageNodeID(storageNodeID) {
		return varlogpb.LogStreamStatusRunning, types.InvalidGLSN, verrors.ErrInvalidArgument
	}
	lse, ok := sn.GetLogStreamExecutor(logStreamID)
	if !ok {
		return varlogpb.LogStreamStatusRunning, types.InvalidGLSN, verrors.ErrInvalidArgument
	}
	status, hwm := lse.Seal(lastCommittedGLSN)
	sn.tst.Touch()
	return status, hwm, nil
}

// Unseal implements the Management Unseal method.
func (sn *StorageNode) Unseal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error {
	if !sn.verifyClusterID(clusterID) || !sn.verifyStorageNodeID(storageNodeID) {
		return verrors.ErrInvalidArgument
	}
	lse, ok := sn.GetLogStreamExecutor(logStreamID)
	if !ok {
		return verrors.ErrInvalidArgument
	}
	sn.tst.Touch()
	return lse.Unseal()
}

func (sn *StorageNode) Sync(ctx context.Context, clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, replica Replica, lastGLSN types.GLSN) (*snpb.SyncStatus, error) {
	if !sn.verifyClusterID(clusterID) || !sn.verifyStorageNodeID(storageNodeID) {
		return nil, verrors.ErrInvalidArgument
	}
	lse, ok := sn.GetLogStreamExecutor(logStreamID)
	if !ok {
		sn.logger.Error("could not get LSE")
		return nil, verrors.ErrInvalidArgument
	}
	sts, err := lse.Sync(ctx, replica, lastGLSN)
	if err != nil {
		return nil, err
	}
	return &snpb.SyncStatus{
		State:   sts.State,
		First:   sts.First,
		Last:    sts.Last,
		Current: sts.Current,
	}, nil
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
