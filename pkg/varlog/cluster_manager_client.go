package varlog

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
)

type ClusterManagerClient interface {
	AddStorageNode(ctx context.Context, addr string) (*varlogpb.StorageNodeMetadataDescriptor, error)
	UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error
	AddLogStream(ctx context.Context, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error)
	UnregisterLogStream(ctx context.Context, logStreamID types.LogStreamID) error
	RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error
	UpdateLogStream(ctx context.Context, logStreamID types.LogStreamID, logStreamReplicas []*varlogpb.ReplicaDescriptor) error
	Seal(ctx context.Context, logStreamID types.LogStreamID) ([]varlogpb.LogStreamMetadataDescriptor, error)
	Unseal(ctx context.Context, logStreamID types.LogStreamID) error
	Sync(ctx context.Context, logStreamID types.LogStreamID, srcStorageNodeId, dstStorageNodeId types.StorageNodeID) (*snpb.SyncStatus, error)
	Close() error
}

var _ ClusterManagerClient = (*clusterManagerClient)(nil)

type clusterManagerClient struct {
	rpcConn   *RpcConn
	rpcClient vmspb.ClusterManagerClient
}

func NewClusterManagerClient(addr string) (ClusterManagerClient, error) {
	rpcConn, err := NewRpcConn(addr)
	if err != nil {
		return nil, err
	}
	cli := &clusterManagerClient{
		rpcConn:   rpcConn,
		rpcClient: vmspb.NewClusterManagerClient(rpcConn.Conn),
	}
	return cli, nil
}

func (c *clusterManagerClient) Close() error {
	return c.rpcConn.Close()
}

func (c *clusterManagerClient) AddStorageNode(ctx context.Context, addr string) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	rsp, err := c.rpcClient.AddStorageNode(ctx, &vmspb.AddStorageNodeRequest{Address: addr})
	if err != nil {
		return nil, FromStatusError(ctx, err)
	}
	return rsp.StorageNode, nil
}

func (c *clusterManagerClient) UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error {
	_, err := c.rpcClient.UnregisterStorageNode(ctx, &vmspb.UnregisterStorageNodeRequest{StorageNodeID: storageNodeID})
	return FromStatusError(ctx, err)
}

func (c *clusterManagerClient) AddLogStream(ctx context.Context, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.AddLogStream(ctx, &vmspb.AddLogStreamRequest{Replicas: logStreamReplicas})
	if err != nil {
		return nil, FromStatusError(ctx, err)
	}
	return rsp.GetLogStream(), nil
}

func (c *clusterManagerClient) UnregisterLogStream(ctx context.Context, logStreamID types.LogStreamID) error {
	_, err := c.rpcClient.UnregisterLogStream(ctx, &vmspb.UnregisterLogStreamRequest{LogStreamID: logStreamID})
	return FromStatusError(ctx, err)
}

func (c *clusterManagerClient) RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error {
	_, err := c.rpcClient.RemoveLogStreamReplica(ctx, &vmspb.RemoveLogStreamReplicaRequest{
		StorageNodeID: storageNodeID,
		LogStreamID:   logStreamID,
	})
	return FromStatusError(ctx, err)
}

func (c *clusterManagerClient) UpdateLogStream(ctx context.Context, logStreamID types.LogStreamID, logStreamReplicas []*varlogpb.ReplicaDescriptor) error {
	_, err := c.rpcClient.UpdateLogStream(ctx, &vmspb.UpdateLogStreamRequest{
		LogStreamID: logStreamID,
		Replicas:    logStreamReplicas,
	})
	return FromStatusError(ctx, err)
}

func (c *clusterManagerClient) Seal(ctx context.Context, logStreamID types.LogStreamID) ([]varlogpb.LogStreamMetadataDescriptor, error) {
	rsp, err := c.rpcClient.Seal(ctx, &vmspb.SealRequest{LogStreamID: logStreamID})
	if err != nil {
		return nil, FromStatusError(ctx, err)
	}
	return rsp.GetLogStreams(), nil
}

func (c *clusterManagerClient) Unseal(ctx context.Context, logStreamID types.LogStreamID) error {
	_, err := c.rpcClient.Unseal(ctx, &vmspb.UnsealRequest{LogStreamID: logStreamID})
	return FromStatusError(ctx, err)
}

func (c *clusterManagerClient) Sync(ctx context.Context, logStreamID types.LogStreamID, srcStorageNodeId, dstStorageNodeId types.StorageNodeID) (*snpb.SyncStatus, error) {
	rsp, err := c.rpcClient.Sync(ctx, &vmspb.SyncRequest{
		LogStreamID:      logStreamID,
		SrcStorageNodeID: srcStorageNodeId,
		DstStorageNodeID: dstStorageNodeId,
	})
	return rsp.GetStatus(), FromStatusError(ctx, err)
}
