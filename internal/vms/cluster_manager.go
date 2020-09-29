package vms

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

// StorStorageNodeSelectionPolicy chooses the storage nodes to add a new log stream.
type StorageNodeSelector interface {
	SelectStorageNode(cluterMeta *vpb.MetadataDescriptor) []types.StorageNodeID
}

type LogStreamIDGenerator interface {
	Generate() types.LogStreamID
}

type ClusterManagerOptions struct {
	ReplicationFactor uint32
}

// ClusterManager manages varlog cluster.
type ClusterManager interface {
	// AddStorageNode adds new StorageNode to the cluster.
	AddStorageNode(ctx context.Context, addr string) error

	AddLogStream(ctx context.Context) error

	// AddLogStream adds new LogStream to the cluster.
	AddLogStreamWith(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) error

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
}

type clusterManager struct {
	snManager      StorageNodeManager
	mrManager      MetadataRepositoryManager
	metaStorage    ClusterMetadataView
	snSelector     StorageNodeSelector
	logStreamIDGen LogStreamIDGenerator

	options *ClusterManagerOptions
}

var _ ClusterManager = (*clusterManager)(nil)

func (cm *clusterManager) AddStorageNode(ctx context.Context, addr string) error {
	snmeta, err := cm.snManager.GetMetadataByAddr(ctx, addr)
	if err != nil {
		return err
	}

	// TODO: Do something by using meta
	//
	//
	if err := cm.mrManager.RegisterStorageNode(ctx, snmeta.GetStorageNode()); err != nil {
		return err
	}
	return nil
}

func (cm *clusterManager) AddLogStream(ctx context.Context) error {
	cmeta, err := cm.metaStorage.ClusterMetadata(ctx)
	if err != nil {
		return err
	}
	storageNodeIDs := cm.snSelector.SelectStorageNode(cmeta)
	logStreamID := cm.logStreamIDGen.Generate()
	logStreamDesc := &vpb.LogStreamDescriptor{
		LogStreamID: logStreamID,
		Status:      vpb.LogStreamStatusRunning,
		Replicas:    make([]*vpb.ReplicaDescriptor, 0, cm.options.ReplicationFactor),
	}
	for _, storageNodeID := range storageNodeIDs {
		logStreamDesc.Replicas = append(logStreamDesc.Replicas, &vpb.ReplicaDescriptor{
			StorageNodeID: storageNodeID,
			// TODO: snSelector can be expanded to choose path
			Path: "",
		})
	}
	// TODO: Choose the primary - e.g., shuffle logStreamReplicaMetas
	return cm.AddLogStreamWith(ctx, logStreamDesc)
}

func (cm *clusterManager) AddLogStreamWith(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) error {
	if err := cm.snManager.AddLogStream(ctx, logStreamDesc); err != nil {
		return err
	}
	return cm.mrManager.RegisterLogStream(ctx, logStreamDesc)
}

func (cm *clusterManager) Seal(ctx context.Context, logStreamID types.LogStreamID) error {
	lastGLSN, err := cm.mrManager.Seal(ctx, logStreamID)
	if err != nil {
		return err
	}
	return cm.snManager.Seal(ctx, logStreamID, lastGLSN)
}

func (cm *clusterManager) Sync(ctx context.Context, logStreamID types.LogStreamID) error {
	return cm.snManager.Sync(ctx, logStreamID)
}

func (cm *clusterManager) Unseal(ctx context.Context, logStreamID types.LogStreamID) error {
	return cm.snManager.Unseal(ctx, logStreamID)
}
