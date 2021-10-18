package admin

//go:generate mockgen -package admin -destination client_mock.go . Client

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"

	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
)

// Client provides various methods to manage the varlog cluster.
type Client interface {
	AddStorageNode(ctx context.Context, addr string) (*vmspb.AddStorageNodeResponse, error)
	UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) (*vmspb.UnregisterStorageNodeResponse, error)
	AddTopic(ctx context.Context) (*vmspb.AddTopicResponse, error)
	UnregisterTopic(ctx context.Context, topicID types.TopicID) (*vmspb.UnregisterTopicResponse, error)
	AddLogStream(ctx context.Context, topicID types.TopicID, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*vmspb.AddLogStreamResponse, error)
	UnregisterLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.UnregisterLogStreamResponse, error)
	RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.RemoveLogStreamReplicaResponse, error)
	UpdateLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, poppedReplica *varlogpb.ReplicaDescriptor, pushedReplica *varlogpb.ReplicaDescriptor) (*vmspb.UpdateLogStreamResponse, error)
	Seal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.SealResponse, error)
	Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.UnsealResponse, error)
	Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, srcStorageNodeID, dstStorageNodeID types.StorageNodeID) (*vmspb.SyncResponse, error)
	GetMRMembers(ctx context.Context) (*vmspb.GetMRMembersResponse, error)
	AddMRPeer(ctx context.Context, raftURL, rpcAddr string) (*vmspb.AddMRPeerResponse, error)
	RemoveMRPeer(ctx context.Context, raftURL string) (*vmspb.RemoveMRPeerResponse, error)
	GetStorageNodes(ctx context.Context) (*vmspb.GetStorageNodesResponse, error)
	Close() error
}

var _ Client = (*clientImpl)(nil)

type clientImpl struct {
	rpcConn   *rpc.Conn
	rpcClient vmspb.ClusterManagerClient
}

func New(ctx context.Context, addr string) (Client, error) {
	rpcConn, err := rpc.NewConn(ctx, addr)
	if err != nil {
		return nil, err
	}
	cli := &clientImpl{
		rpcConn:   rpcConn,
		rpcClient: vmspb.NewClusterManagerClient(rpcConn.Conn),
	}
	return cli, nil
}

func (c *clientImpl) Close() error {
	return c.rpcConn.Close()
}

func (c *clientImpl) AddStorageNode(ctx context.Context, addr string) (*vmspb.AddStorageNodeResponse, error) {
	rsp, err := c.rpcClient.AddStorageNode(ctx, &vmspb.AddStorageNodeRequest{Address: addr})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) (*vmspb.UnregisterStorageNodeResponse, error) {
	rsp, err := c.rpcClient.UnregisterStorageNode(ctx, &vmspb.UnregisterStorageNodeRequest{StorageNodeID: storageNodeID})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) AddTopic(ctx context.Context) (*vmspb.AddTopicResponse, error) {
	rsp, err := c.rpcClient.AddTopic(ctx, &vmspb.AddTopicRequest{})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) UnregisterTopic(ctx context.Context, topicID types.TopicID) (*vmspb.UnregisterTopicResponse, error) {
	rsp, err := c.rpcClient.UnregisterTopic(ctx, &vmspb.UnregisterTopicRequest{TopicID: topicID})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) AddLogStream(ctx context.Context, topicID types.TopicID, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*vmspb.AddLogStreamResponse, error) {
	rsp, err := c.rpcClient.AddLogStream(ctx, &vmspb.AddLogStreamRequest{
		TopicID:  topicID,
		Replicas: logStreamReplicas,
	})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) UnregisterLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.UnregisterLogStreamResponse, error) {
	rsp, err := c.rpcClient.UnregisterLogStream(ctx, &vmspb.UnregisterLogStreamRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
	})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.RemoveLogStreamReplicaResponse, error) {
	rsp, err := c.rpcClient.RemoveLogStreamReplica(ctx, &vmspb.RemoveLogStreamReplicaRequest{
		StorageNodeID: storageNodeID,
		TopicID:       topicID,
		LogStreamID:   logStreamID,
	})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) UpdateLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, poppedReplica, pushedReplica *varlogpb.ReplicaDescriptor) (*vmspb.UpdateLogStreamResponse, error) {
	rsp, err := c.rpcClient.UpdateLogStream(ctx, &vmspb.UpdateLogStreamRequest{
		TopicID:       topicID,
		LogStreamID:   logStreamID,
		PoppedReplica: poppedReplica,
		PushedReplica: pushedReplica,
	})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) Seal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.SealResponse, error) {
	rsp, err := c.rpcClient.Seal(ctx, &vmspb.SealRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
	})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.UnsealResponse, error) {
	rsp, err := c.rpcClient.Unseal(ctx, &vmspb.UnsealRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
	})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, srcStorageNodeID, dstStorageNodeID types.StorageNodeID) (*vmspb.SyncResponse, error) {
	rsp, err := c.rpcClient.Sync(ctx, &vmspb.SyncRequest{
		TopicID:          topicID,
		LogStreamID:      logStreamID,
		SrcStorageNodeID: srcStorageNodeID,
		DstStorageNodeID: dstStorageNodeID,
	})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) GetMRMembers(ctx context.Context) (*vmspb.GetMRMembersResponse, error) {
	rsp, err := c.rpcClient.GetMRMembers(ctx, &pbtypes.Empty{})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) AddMRPeer(ctx context.Context, raftURL, rpcAddr string) (*vmspb.AddMRPeerResponse, error) {
	rsp, err := c.rpcClient.AddMRPeer(ctx, &vmspb.AddMRPeerRequest{RaftURL: raftURL, RPCAddr: rpcAddr})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) RemoveMRPeer(ctx context.Context, raftURL string) (*vmspb.RemoveMRPeerResponse, error) {
	rsp, err := c.rpcClient.RemoveMRPeer(ctx, &vmspb.RemoveMRPeerRequest{RaftURL: raftURL})
	return rsp, verrors.FromStatusError(err)
}

func (c *clientImpl) GetStorageNodes(ctx context.Context) (*vmspb.GetStorageNodesResponse, error) {
	rsp, err := c.rpcClient.GetStorageNodes(ctx, &pbtypes.Empty{})
	return rsp, verrors.FromStatusError(err)
}
