package varlog

//go:generate mockgen -package varlog -destination admin_mock.go . Admin

import (
	"context"
	"errors"

	pbtypes "github.com/gogo/protobuf/types"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/proto/vmspb"
)

// Admin provides various methods to manage the varlog cluster.
type Admin interface {
	// TODO (jun): Specify types of errors, for instance, retriable, bad request, server's internal error.

	// GetStorageNode returns the metadata of the storage node specified by the argument snid.
	GetStorageNode(ctx context.Context, snid types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error)
	// ListStorageNodes returns a map of StorageNodeIDs and their addresses.
	ListStorageNodes(ctx context.Context) (map[types.StorageNodeID]*snpb.StorageNodeMetadataDescriptor, error)
	// GetStorageNodes returns a map of StorageNodeIDs and their addresses.
	// Deprecated: Use ListStorageNodes.
	GetStorageNodes(ctx context.Context) (map[types.StorageNodeID]*snpb.StorageNodeMetadataDescriptor, error)
	// AddStorageNode registers a storage node to the cluster.
	AddStorageNode(ctx context.Context, snid types.StorageNodeID, addr string) (*snpb.StorageNodeMetadataDescriptor, error)
	// UnregisterStorageNode unregisters a storage node identified by the argument storageNodeID
	// from the cluster.
	UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error

	GetTopic(ctx context.Context, tpid types.TopicID) (*varlogpb.TopicDescriptor, error)
	ListTopics(ctx context.Context) ([]varlogpb.TopicDescriptor, error)
	// Topics returns a list of topics.
	// Deprecated: Use ListTopics.
	Topics(ctx context.Context) ([]varlogpb.TopicDescriptor, error)
	// AddTopic adds a new topic and returns its metadata.
	AddTopic(ctx context.Context) (varlogpb.TopicDescriptor, error)
	// UnregisterTopic removes a topic identified by the argument TopicID from the cluster.
	UnregisterTopic(ctx context.Context, topicID types.TopicID) (*vmspb.UnregisterTopicResponse, error)

	GetLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (*varlogpb.LogStreamDescriptor, error)
	ListLogStreams(ctx context.Context, tpid types.TopicID) ([]*varlogpb.LogStreamDescriptor, error)
	// DescribeTopic returns detailed metadata of the topic.
	// Deprecated: Use ListLogStreams.
	DescribeTopic(ctx context.Context, topicID types.TopicID) (*vmspb.DescribeTopicResponse, error)
	// AddLogStream adds a new log stream to the topic identified by the argument topicID.
	AddLogStream(ctx context.Context, topicID types.TopicID, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error)
	// UpdateLogStream changes replicas of the log stream.
	UpdateLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, poppedReplica *varlogpb.ReplicaDescriptor, pushedReplica *varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error)
	// UnregisterLogStream unregisters a log stream from the cluster.
	UnregisterLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) error

	// RemoveLogStreamReplica removes a log stream replica from the storage node.
	RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID) error

	// Seal seals the log stream identified by the argument topicID and logStreamID.
	Seal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.SealResponse, error)
	// Unseal unseals the log stream identified by the argument topicID and logStreamID.
	Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*varlogpb.LogStreamDescriptor, error)
	// Sync copies logs of the log stream identified by the argument topicID and logStreamID
	// from the source storage node to the destination storage node.
	Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, srcStorageNodeID, dstStorageNodeID types.StorageNodeID) (*snpb.SyncStatus, error)
	// Trim deletes logs whose GLSNs are less than or equal to the argument lastGLSN.
	// Note that the return type of this method can be changed soon.
	Trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN) (map[types.LogStreamID]map[types.StorageNodeID]error, error)

	GetMetadataRepositoryNode(ctx context.Context, nid types.NodeID) (*varlogpb.MetadataRepositoryNode, error)
	ListMetadataRepositoryNodes(ctx context.Context) ([]*varlogpb.MetadataRepositoryNode, error)
	// GetMRMembers returns metadata repositories of the cluster.
	GetMRMembers(ctx context.Context) (*vmspb.GetMRMembersResponse, error)
	AddMetadataRepositoryNode(ctx context.Context, raftURL, rpcAddr string) (*varlogpb.MetadataRepositoryNode, error)
	// AddMRPeer registers a new metadata repository to the cluster.
	AddMRPeer(ctx context.Context, raftURL, rpcAddr string) (types.NodeID, error)
	DeleteMetadataRepositoryNode(ctx context.Context, nid types.NodeID) error
	// RemoveMRPeer unregisters the metadata repository from the cluster.
	RemoveMRPeer(ctx context.Context, raftURL string) error

	// Close closes a connection to the admin server.
	// Once this method is called, the Client can't be used anymore.
	Close() error
}

var _ Admin = (*admin)(nil)

type admin struct {
	rpcConn   *rpc.Conn
	rpcClient vmspb.ClusterManagerClient
}

// NewAdmin creates Admin that connects to admin server by using the argument addr.
func NewAdmin(ctx context.Context, addr string) (Admin, error) {
	rpcConn, err := rpc.NewConn(ctx, addr)
	if err != nil {
		return nil, err
	}
	cli := &admin{
		rpcConn:   rpcConn,
		rpcClient: vmspb.NewClusterManagerClient(rpcConn.Conn),
	}
	return cli, nil
}

func (c *admin) Close() error {
	return c.rpcConn.Close()
}

func (c *admin) GetStorageNode(ctx context.Context, snid types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error) {
	rsp, err := c.rpcClient.GetStorageNode(ctx, &vmspb.GetStorageNodeRequest{
		StorageNodeID: snid,
	})
	return rsp.GetStorageNode(), err
}

func (c *admin) ListStorageNodes(ctx context.Context) (map[types.StorageNodeID]*snpb.StorageNodeMetadataDescriptor, error) {
	rsp, err := c.rpcClient.ListStorageNodes(ctx, &vmspb.ListStorageNodesRequest{})
	return rsp.GetStorageNodes(), verrors.FromStatusError(err)
}

func (c *admin) GetStorageNodes(ctx context.Context) (map[types.StorageNodeID]*snpb.StorageNodeMetadataDescriptor, error) {
	return c.ListStorageNodes(ctx)
}

func (c *admin) AddStorageNode(ctx context.Context, snid types.StorageNodeID, addr string) (*snpb.StorageNodeMetadataDescriptor, error) {
	rsp, err := c.rpcClient.AddStorageNode(ctx, &vmspb.AddStorageNodeRequest{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: snid,
			Address:       addr,
		},
	})
	return rsp.GetStorageNode(), verrors.FromStatusError(err)
}

func (c *admin) UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error {
	_, err := c.rpcClient.UnregisterStorageNode(ctx, &vmspb.UnregisterStorageNodeRequest{StorageNodeID: storageNodeID})
	return verrors.FromStatusError(err)
}

func (c *admin) GetTopic(ctx context.Context, tpid types.TopicID) (*varlogpb.TopicDescriptor, error) {
	rsp, err := c.rpcClient.GetTopic(ctx, &vmspb.GetTopicRequest{
		TopicID: tpid,
	})
	return rsp.GetTopic(), err
}

func (c *admin) ListTopics(ctx context.Context) ([]varlogpb.TopicDescriptor, error) {
	rsp, err := c.rpcClient.ListTopics(ctx, &vmspb.ListTopicsRequest{})
	return rsp.GetTopics(), verrors.FromStatusError(err)
}

func (c *admin) Topics(ctx context.Context) ([]varlogpb.TopicDescriptor, error) {
	return c.ListTopics(ctx)
}

func (c *admin) AddTopic(ctx context.Context) (varlogpb.TopicDescriptor, error) {
	rsp, err := c.rpcClient.AddTopic(ctx, &vmspb.AddTopicRequest{})
	if err != nil {
		return varlogpb.TopicDescriptor{}, verrors.FromStatusError(err)
	}
	return rsp.Topic, nil
}

func (c *admin) UnregisterTopic(ctx context.Context, topicID types.TopicID) (*vmspb.UnregisterTopicResponse, error) {
	rsp, err := c.rpcClient.UnregisterTopic(ctx, &vmspb.UnregisterTopicRequest{TopicID: topicID})
	return rsp, verrors.FromStatusError(err)
}

func (c *admin) GetLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.GetLogStream(ctx, &vmspb.GetLogStreamRequest{
		TopicID:     tpid,
		LogStreamID: lsid,
	})
	return rsp.GetLogStream(), err
}

func (c *admin) ListLogStreams(ctx context.Context, tpid types.TopicID) ([]*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.ListLogStreams(ctx, &vmspb.ListLogStreamsRequest{
		TopicID: tpid,
	})
	return rsp.GetLogStreams(), err
}

func (c *admin) DescribeTopic(ctx context.Context, topicID types.TopicID) (*vmspb.DescribeTopicResponse, error) {
	rsp, err := c.rpcClient.DescribeTopic(ctx, &vmspb.DescribeTopicRequest{TopicID: topicID})
	return rsp, verrors.FromStatusError(err)
}

func (c *admin) AddLogStream(ctx context.Context, topicID types.TopicID, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.AddLogStream(ctx, &vmspb.AddLogStreamRequest{
		TopicID:  topicID,
		Replicas: logStreamReplicas,
	})
	return rsp.GetLogStream(), verrors.FromStatusError(err)
}

func (c *admin) UpdateLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, poppedReplica, pushedReplica *varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.UpdateLogStream(ctx, &vmspb.UpdateLogStreamRequest{
		TopicID:       topicID,
		LogStreamID:   logStreamID,
		PoppedReplica: poppedReplica,
		PushedReplica: pushedReplica,
	})
	return rsp.GetLogStream(), verrors.FromStatusError(err)
}

func (c *admin) UnregisterLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) error {
	_, err := c.rpcClient.UnregisterLogStream(ctx, &vmspb.UnregisterLogStreamRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
	})
	return verrors.FromStatusError(err)
}

func (c *admin) RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID) error {
	_, err := c.rpcClient.RemoveLogStreamReplica(ctx, &vmspb.RemoveLogStreamReplicaRequest{
		StorageNodeID: storageNodeID,
		TopicID:       topicID,
		LogStreamID:   logStreamID,
	})
	return verrors.FromStatusError(err)
}

func (c *admin) Seal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.SealResponse, error) {
	rsp, err := c.rpcClient.Seal(ctx, &vmspb.SealRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
	})
	return rsp, verrors.FromStatusError(err)
}

func (c *admin) Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.Unseal(ctx, &vmspb.UnsealRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
	})
	return rsp.GetLogStream(), verrors.FromStatusError(err)
}

func (c *admin) Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, srcStorageNodeID, dstStorageNodeID types.StorageNodeID) (*snpb.SyncStatus, error) {
	rsp, err := c.rpcClient.Sync(ctx, &vmspb.SyncRequest{
		TopicID:          topicID,
		LogStreamID:      logStreamID,
		SrcStorageNodeID: srcStorageNodeID,
		DstStorageNodeID: dstStorageNodeID,
	})
	return rsp.GetStatus(), verrors.FromStatusError(err)
}

func (c *admin) Trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN) (map[types.LogStreamID]map[types.StorageNodeID]error, error) {
	rsp, err := c.rpcClient.Trim(ctx, &vmspb.TrimRequest{
		TopicID:  topicID,
		LastGLSN: lastGLSN,
	})
	if err != nil {
		return nil, err
	}
	ret := make(map[types.LogStreamID]map[types.StorageNodeID]error)
	for _, result := range rsp.Results {
		lsid := result.LogStreamID
		if _, ok := ret[lsid]; !ok {
			ret[lsid] = make(map[types.StorageNodeID]error)
		}
		var err error
		if len(result.Error) > 0 {
			err = errors.New(result.Error)
		}
		ret[lsid][result.StorageNodeID] = err
	}
	return ret, nil
}

func (c *admin) GetMetadataRepositoryNode(ctx context.Context, nid types.NodeID) (*varlogpb.MetadataRepositoryNode, error) {
	rsp, err := c.rpcClient.GetMetadataRepositoryNode(ctx, &vmspb.GetMetadataRepositoryNodeRequest{
		NodeID: nid,
	})
	return rsp.GetNode(), err
}

func (c *admin) ListMetadataRepositoryNodes(ctx context.Context) ([]*varlogpb.MetadataRepositoryNode, error) {
	rsp, err := c.rpcClient.ListMetadataRepositoryNodes(ctx, &vmspb.ListMetadataRepositoryNodesRequest{})
	return rsp.GetNodes(), err
}

func (c *admin) GetMRMembers(ctx context.Context) (*vmspb.GetMRMembersResponse, error) {
	rsp, err := c.rpcClient.GetMRMembers(ctx, &pbtypes.Empty{})
	return rsp, verrors.FromStatusError(err)
}

func (c *admin) AddMetadataRepositoryNode(ctx context.Context, raftURL, rpcAddr string) (*varlogpb.MetadataRepositoryNode, error) {
	rsp, err := c.rpcClient.AddMetadataRepositoryNode(ctx, &vmspb.AddMetadataRepositoryNodeRequest{
		RaftURL: raftURL,
		RPCAddr: rpcAddr,
	})
	return rsp.GetNode(), err
}

func (c *admin) AddMRPeer(ctx context.Context, raftURL, rpcAddr string) (types.NodeID, error) {
	rsp, err := c.rpcClient.AddMRPeer(ctx, &vmspb.AddMRPeerRequest{RaftURL: raftURL, RPCAddr: rpcAddr})
	return rsp.GetNodeID(), verrors.FromStatusError(err)
}

func (c *admin) DeleteMetadataRepositoryNode(ctx context.Context, nid types.NodeID) error {
	_, err := c.rpcClient.DeleteMetadataRepositoryNode(ctx, &vmspb.DeleteMetadataRepositoryNodeRequest{
		NodeID: nid,
	})
	return err
}

func (c *admin) RemoveMRPeer(ctx context.Context, raftURL string) error {
	_, err := c.rpcClient.RemoveMRPeer(ctx, &vmspb.RemoveMRPeerRequest{RaftURL: raftURL})
	return verrors.FromStatusError(err)
}
