package storage

import (
	"context"
	"net"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/netutil"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/runner/stopwaiter"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/syncutil"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
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
}

type LogStreamExecutorGetter interface {
	GetLogStreamExecutor(logStreamID types.LogStreamID) (LogStreamExecutor, bool)
	GetLogStreamExecutors() []LogStreamExecutor
}

type StorageNode struct {
	clusterID     types.ClusterID
	storageNodeID types.StorageNodeID

	lseMtx sync.RWMutex
	lseMap map[types.LogStreamID]LogStreamExecutor

	lsr LogStreamReporter

	options *StorageNodeOptions
	logger  *zap.Logger

	sw     *stopwaiter.StopWaiter
	runner runner.Runner
	// FIXME (jun): remove context
	runnerContext context.Context
	cancel        context.CancelFunc
	muCancel      sync.Mutex
	once          syncutil.OnlyOnce

	server *grpc.Server

	// FIXME (jun): serverAddr is not necessary, it is needed only for tests.
	serverAddr string
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
	sn.lsr = NewLogStreamReporter(sn.logger, sn.storageNodeID, sn, &options.LogStreamReporterOptions)
	sn.server = grpc.NewServer()

	NewLogStreamReporterService(sn.lsr).Register(sn.server)
	NewLogIOService(sn.storageNodeID, sn).Register(sn.server)
	NewManagementService(sn.logger, sn).Register(sn.server)

	return sn, nil
}

func (sn *StorageNode) Run() error {
	return sn.once.Do(func() error {
		ctx, cancel := context.WithCancel(context.Background())
		sn.runnerContext = ctx
		sn.muCancel.Lock()
		sn.cancel = cancel
		sn.muCancel.Unlock()
		sn.runner.RunDeprecated(ctx, sn.lsr.Run)

		sn.logger.Info("listening", zap.String("address", sn.options.RPCBindAddress))
		lis, err := net.Listen("tcp", sn.options.RPCBindAddress)
		if err != nil {
			sn.logger.Error("could not listen", zap.Error(err))
			return err
		}
		sn.serverAddr, _ = netutil.GetListenerLocalAddr(lis)

		go func() {
			if err := sn.server.Serve(lis); err != nil {
				sn.logger.Error("could not serve", zap.Error(err))
				sn.Close()
			}
		}()

		sn.logger.Info("starting storagenode")
		return nil
	})
}

func (sn *StorageNode) Close() error {
	sn.muCancel.Lock()
	defer sn.muCancel.Unlock()
	if sn.cancel != nil {
		sn.cancel()
		sn.lsr.Close()
		for _, lse := range sn.GetLogStreamExecutors() {
			lse.Close()
		}
		sn.runner.CloseWaitDeprecated()
		sn.sw.Stop()
	}
	return nil
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
	sn.lseMap[lsid] = lse
	sn.runner.RunDeprecated(sn.runnerContext, lse.Run)
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
