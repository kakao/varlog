package varlog

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/proto/vmspb"
)

type ClusterManagerClient interface {
	AddStorageNode(ctx context.Context, addr string) (*vmspb.AddStorageNodeResponse, error)
	UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) (*vmspb.UnregisterStorageNodeResponse, error)
	AddLogStream(ctx context.Context, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*vmspb.AddLogStreamResponse, error)
	UnregisterLogStream(ctx context.Context, logStreamID types.LogStreamID) (*vmspb.UnregisterLogStreamResponse, error)
	RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) (*vmspb.RemoveLogStreamReplicaResponse, error)
	UpdateLogStream(ctx context.Context, logStreamID types.LogStreamID, poppedReplica *varlogpb.ReplicaDescriptor, pushedReplica *varlogpb.ReplicaDescriptor) (*vmspb.UpdateLogStreamResponse, error)
	Seal(ctx context.Context, logStreamID types.LogStreamID) (*vmspb.SealResponse, error)
	Unseal(ctx context.Context, logStreamID types.LogStreamID) (*vmspb.UnsealResponse, error)
	Sync(ctx context.Context, logStreamID types.LogStreamID, srcStorageNodeId, dstStorageNodeId types.StorageNodeID) (*vmspb.SyncResponse, error)
	GetMRMembers(ctx context.Context) (*vmspb.GetMRMembersResponse, error)
	Close() error
}

var _ ClusterManagerClient = (*clusterManagerClient)(nil)

type clusterManagerClient struct {
	rpcConn   *rpc.Conn
	rpcClient vmspb.ClusterManagerClient
}

func NewClusterManagerClient(addr string) (ClusterManagerClient, error) {
	rpcConn, err := rpc.NewConn(addr)
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

func (c *clusterManagerClient) AddStorageNode(ctx context.Context, addr string) (*vmspb.AddStorageNodeResponse, error) {
	rsp, err := c.rpcClient.AddStorageNode(ctx, &vmspb.AddStorageNodeRequest{Address: addr})
	return rsp, verrors.FromStatusError(ctx, err)
}

func (c *clusterManagerClient) UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) (*vmspb.UnregisterStorageNodeResponse, error) {
	rsp, err := c.rpcClient.UnregisterStorageNode(ctx, &vmspb.UnregisterStorageNodeRequest{StorageNodeID: storageNodeID})
	return rsp, verrors.FromStatusError(ctx, err)
}

func (c *clusterManagerClient) AddLogStream(ctx context.Context, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*vmspb.AddLogStreamResponse, error) {
	rsp, err := c.rpcClient.AddLogStream(ctx, &vmspb.AddLogStreamRequest{Replicas: logStreamReplicas})
	return rsp, verrors.FromStatusError(ctx, err)
}

func (c *clusterManagerClient) UnregisterLogStream(ctx context.Context, logStreamID types.LogStreamID) (*vmspb.UnregisterLogStreamResponse, error) {
	rsp, err := c.rpcClient.UnregisterLogStream(ctx, &vmspb.UnregisterLogStreamRequest{LogStreamID: logStreamID})
	return rsp, verrors.FromStatusError(ctx, err)
}

func (c *clusterManagerClient) RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) (*vmspb.RemoveLogStreamReplicaResponse, error) {
	rsp, err := c.rpcClient.RemoveLogStreamReplica(ctx, &vmspb.RemoveLogStreamReplicaRequest{
		StorageNodeID: storageNodeID,
		LogStreamID:   logStreamID,
	})
	return rsp, verrors.FromStatusError(ctx, err)
}

func (c *clusterManagerClient) UpdateLogStream(ctx context.Context, logStreamID types.LogStreamID, poppedReplica, pushedReplica *varlogpb.ReplicaDescriptor) (*vmspb.UpdateLogStreamResponse, error) {
	rsp, err := c.rpcClient.UpdateLogStream(ctx, &vmspb.UpdateLogStreamRequest{
		LogStreamID:   logStreamID,
		PoppedReplica: poppedReplica,
		PushedReplica: pushedReplica,
	})
	return rsp, verrors.FromStatusError(ctx, err)
}

func (c *clusterManagerClient) Seal(ctx context.Context, logStreamID types.LogStreamID) (*vmspb.SealResponse, error) {
	rsp, err := c.rpcClient.Seal(ctx, &vmspb.SealRequest{LogStreamID: logStreamID})
	return rsp, verrors.FromStatusError(ctx, err)
}

func (c *clusterManagerClient) Unseal(ctx context.Context, logStreamID types.LogStreamID) (*vmspb.UnsealResponse, error) {
	rsp, err := c.rpcClient.Unseal(ctx, &vmspb.UnsealRequest{LogStreamID: logStreamID})
	return rsp, verrors.FromStatusError(ctx, err)
}

func (c *clusterManagerClient) Sync(ctx context.Context, logStreamID types.LogStreamID, srcStorageNodeId, dstStorageNodeId types.StorageNodeID) (*vmspb.SyncResponse, error) {
	rsp, err := c.rpcClient.Sync(ctx, &vmspb.SyncRequest{
		LogStreamID:      logStreamID,
		SrcStorageNodeID: srcStorageNodeId,
		DstStorageNodeID: dstStorageNodeId,
	})
	return rsp, verrors.FromStatusError(ctx, err)
}

func (c *clusterManagerClient) GetMRMembers(ctx context.Context) (*vmspb.GetMRMembersResponse, error) {
	rsp, err := c.rpcClient.GetMRMembers(ctx, &pbtypes.Empty{})
	return rsp, verrors.FromStatusError(ctx, err)
}
