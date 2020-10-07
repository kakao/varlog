package vms

import (
	"context"
	"errors"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
	"go.uber.org/zap"
)

var (
	errCMVNoStorageNode = errors.New("cmview: no such storage node")
)

// ClusterMetadataView is the storage to store varlog cluster. It provides the latest metadata
// about the cluster.
type ClusterMetadataView interface {
	// ClusterMetadata returns the latest metadata of the cluster.
	ClusterMetadata(ctx context.Context) (*vpb.MetadataDescriptor, error)

	// StorageNode returns the storage node corresponded with the storageNodeID.
	StorageNode(ctx context.Context, storageNodeID types.StorageNodeID) (*vpb.StorageNodeDescriptor, error)

	// LogStreamReplicas returns all of the latest LogStreamReplicaMetas for the given
	// logStreamID. The first element of the returned LogStreamReplicaMeta list is the primary
	// LogStreamReplica.
	LogStreamReplicas(ctx context.Context, logStreamID types.LogStreamID) ([]*vpb.LogStreamMetadataDescriptor, error)
}

type clusterMetadataView struct {
	meta      vpb.MetadataDescriptor
	mrManager MetadataRepositoryManager

	logger *zap.Logger
}

var _ ClusterMetadataView = (*clusterMetadataView)(nil)

func NewClusterMetadataView(mrManager MetadataRepositoryManager, logger *zap.Logger) ClusterMetadataView {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("cmview")
	return &clusterMetadataView{
		mrManager: mrManager,
		logger:    logger,
	}
}

func (cmv *clusterMetadataView) ClusterMetadata(ctx context.Context) (*vpb.MetadataDescriptor, error) {
	return cmv.mrManager.GetClusterMetadata(ctx)
}

func (cmv *clusterMetadataView) StorageNode(ctx context.Context, storageNodeID types.StorageNodeID) (*vpb.StorageNodeDescriptor, error) {
	meta, err := cmv.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	if sndesc := meta.GetStorageNode(storageNodeID); sndesc != nil {
		return sndesc, nil
	}
	return nil, errCMVNoStorageNode
}

func (cmv *clusterMetadataView) LogStreamReplicas(ctx context.Context, logStreamID types.LogStreamID) ([]*vpb.LogStreamMetadataDescriptor, error) {
	panic("not implemented")
}
