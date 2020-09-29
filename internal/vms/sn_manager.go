package vms

import (
	"context"
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	vpb "github.com/kakao/varlog/proto/varlog"
	"go.uber.org/zap"
)

type StorageNodeManager interface {
	// GetMetadataByAddr returns metadata about a storage node. It is useful when id of the
	// storage node is not known.
	GetMetadataByAddr(ctx context.Context, addr string) (*vpb.StorageNodeMetadataDescriptor, error)

	AddLogStream(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) error

	Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) error

	Sync(ctx context.Context, logStreamID types.LogStreamID) error

	Unseal(ctx context.Context, logStreamID types.LogStreamID) error
}

type storageNodeManager struct {
	metaStorage ClusterMetadataView
	mcls        map[types.StorageNodeID]varlog.ManagementClient
	mu          sync.Mutex
	logger      *zap.Logger
}

var _ StorageNodeManager = (*storageNodeManager)(nil)

func NewStorageNodeManager(metaStorage ClusterMetadataView, logger *zap.Logger) StorageNodeManager {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("storagenodemanager")
	return &storageNodeManager{
		metaStorage: metaStorage,
		mcls:        make(map[types.StorageNodeID]varlog.ManagementClient),
		logger:      logger,
	}
}

func (sm *storageNodeManager) GetMetadataByAddr(ctx context.Context, addr string) (*vpb.StorageNodeMetadataDescriptor, error) {
	panic("not implemented")
}

func (sm *storageNodeManager) AddLogStream(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) error {
	panic("not implemented")
}

func (sm *storageNodeManager) Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) error {
	panic("not implemented")
}

func (sm *storageNodeManager) Sync(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
}

func (sm *storageNodeManager) Unseal(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
}
