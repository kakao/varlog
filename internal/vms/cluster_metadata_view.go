package vms

import (
	"context"
	"errors"
	"sync"

	"github.com/kakao/varlog/pkg/varlog/types"
	vpb "github.com/kakao/varlog/proto/varlog"
	"go.uber.org/zap"
)

var (
	errCMVNoStorageNode = errors.New("cmview: no such storage node")
	errCMVNoLogStream   = errors.New("cmview: no such log stream")
)

// ClusterMetadataView provides the latest metadata about the cluster.
// TODO: It should have a way to guarantee that ClusterMetadata is the latest.
// TODO: See https://github.com/kakao/varlog/pull/198#discussion_r215542
type ClusterMetadataView interface {
	SetDirty()

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
	mu         sync.RWMutex
	meta       *vpb.MetadataDescriptor
	dirty      bool
	metaGetter MetadataGetter

	logger *zap.Logger
}

var _ ClusterMetadataView = (*clusterMetadataView)(nil)

// Do not call this directly. Use (MetadataRepositoryManager).ClusterMetadataView().
func newClusterMetadataView(metaGetter MetadataGetter, logger *zap.Logger) ClusterMetadataView {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("cmview")
	return &clusterMetadataView{
		metaGetter: metaGetter,
		logger:     logger,
		dirty:      true,
	}
}

func (cmv *clusterMetadataView) SetDirty() {
	cmv.mu.Lock()
	defer cmv.mu.Unlock()
	cmv.dirty = true
}

func (cmv *clusterMetadataView) ClusterMetadata(ctx context.Context) (*vpb.MetadataDescriptor, error) {
	cmv.mu.Lock()
	defer cmv.mu.Unlock()

	// FIXME: to avoid long locking, use timeout
	if cmv.dirty {
		// FIXME (jun): How can we convince if the meta is the latest?
		// If MRManager registers new log stream and sets dirty, and then the MR node
		// failed, MRManager can connect to other MR node which is not yet applied.
		// See https://github.com/kakao/varlog/pull/198#discussion_r215542
		meta, err := cmv.metaGetter.GetClusterMetadata(ctx)
		if err != nil {
			return nil, err
		}
		cmv.meta = meta
		cmv.dirty = false
	}
	return cmv.meta, nil
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
