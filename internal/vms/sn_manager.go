package vms

import (
	"context"
	"errors"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
	"go.uber.org/zap"
)

type StorageNodeManager interface {
	// GetMetadataByAddr returns metadata about a storage node. It is useful when id of the
	// storage node is not known.
	GetMetadataByAddr(ctx context.Context, addr string) (varlog.ManagementClient, *vpb.StorageNodeMetadataDescriptor, error)

	AddStorageNode(ctx context.Context, snmcl varlog.ManagementClient) error

	AddLogStream(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) error

	Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) error

	Sync(ctx context.Context, logStreamID types.LogStreamID) error

	Unseal(ctx context.Context, logStreamID types.LogStreamID) error

	Close() error
}

var _ StorageNodeManager = (*snManager)(nil)

type snManager struct {
	clusterID types.ClusterID

	cs     map[types.StorageNodeID]varlog.ManagementClient
	cmView ClusterMetadataView
	mu     sync.RWMutex

	logger *zap.Logger
}

func NewStorageNodeManager(cmView ClusterMetadataView, logger *zap.Logger) StorageNodeManager {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("snmanager")
	return &snManager{
		cmView: cmView,
		cs:     make(map[types.StorageNodeID]varlog.ManagementClient),
		logger: logger,
	}
}

func (sm *snManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var err error
	for id, cli := range sm.cs {
		if e := cli.Close(); e != nil {
			sm.logger.Error("could not close storagenode management client", zap.Any("snid", id), zap.Error(e))
			err = e
		}
	}
	return err
}

func (sm *snManager) GetMetadataByAddr(ctx context.Context, addr string) (varlog.ManagementClient, *vpb.StorageNodeMetadataDescriptor, error) {
	mc, err := varlog.NewManagementClient(addr)
	if err != nil {
		sm.logger.Error("could not create storagenode management client", zap.Error(err))
		return nil, nil, err
	}
	snMeta, err := mc.GetMetadata(ctx, sm.clusterID, snpb.MetadataTypeHeartbeat)
	if err != nil {
		if err := mc.Close(); err != nil {
			sm.logger.Error("could not close storagenode management client", zap.Error(err))
			return nil, nil, err
		}
		return nil, nil, err
	}
	return mc, snMeta, nil
}

func (sm *snManager) AddStorageNode(ctx context.Context, snmcl varlog.ManagementClient) error {
	snMeta, err := snmcl.GetMetadata(ctx, sm.clusterID, snpb.MetadataTypeHeartbeat)
	if err != nil {
		return err
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.cs[snMeta.StorageNode.StorageNodeID]; ok {
		// TODO (jun): already exists
		// compare snMeta with already existing one, and then handle exceptional cases
	}

	sm.cs[snMeta.StorageNode.StorageNodeID] = snmcl
	return err
}

func (sm *snManager) AddLogStream(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	logStreamID := logStreamDesc.GetLogStreamID()
	replicaDescList := logStreamDesc.GetReplicas()
	for _, replicaDesc := range replicaDescList {
		storageNodeID := replicaDesc.GetStorageNodeID()
		cli, ok := sm.cs[storageNodeID]
		if !ok {
			sm.logger.Panic("storagenodemanager: no such storage node", zap.Any("snid", storageNodeID))
		}

		// TODO (jun): Currently, it raises an error when the logStreamID already exists.
		// It is okay?
		snmeta, err := cli.GetMetadata(ctx, sm.clusterID, snpb.MetadataTypeLogStreams)
		if err != nil {
			return err
		}
		if _, found := snmeta.FindLogStream(logStreamID); found {
			return errors.New("vms: alredy exist logstream")
		}

		if err := cli.AddLogStream(ctx, sm.clusterID, storageNodeID, logStreamID, replicaDesc.GetPath()); err != nil {
			return err
		}
	}
	return nil
}

func (sm *snManager) Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) error {
	panic("not implemented")
}

func (sm *snManager) Sync(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
}

func (sm *snManager) Unseal(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
}
