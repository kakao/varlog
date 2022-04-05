package varlog

//go:generate mockgen -package varlog -destination admin_mock.go . Admin

import (
	"context"
	"errors"

	pbtypes "github.com/gogo/protobuf/types"

	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
)

// Admin provides various methods to manage the varlog cluster.
type Admin interface {
	// AddStorageNode registers a storage node to the cluster.
	AddStorageNode(ctx context.Context, addr string) (*varlogpb.StorageNodeMetadataDescriptor, error)

	// UnregisterStorageNode unregisters a storage node identified by the argument storageNodeID
	// from the cluster.
	UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error
	// AddTopic adds a new topic and returns its metadata.
	AddTopic(ctx context.Context) (varlogpb.TopicDescriptor, error)

	// Topics returns a list of topics.
	Topics(ctx context.Context) ([]varlogpb.TopicDescriptor, error)

	// DescribeTopic returns detailed metadata of the topic.
	DescribeTopic(ctx context.Context, topicID types.TopicID) (*vmspb.DescribeTopicResponse, error)

	// UnregisterTopic removes a topic identified by the argument TopicID from the cluster.
	UnregisterTopic(ctx context.Context, topicID types.TopicID) (*vmspb.UnregisterTopicResponse, error)

	// AddLogStream adds a new log stream to the topic identified by the argument topicID.
	AddLogStream(ctx context.Context, topicID types.TopicID, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error)

	// UnregisterLogStream unregisters a log stream from the cluster.
	UnregisterLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) error

	// RemoveLogStreamReplica removes a log stream replica from the storage node.
	RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID) error

	// UpdateLogStream changes replicas of the log stream.
	UpdateLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, poppedReplica *varlogpb.ReplicaDescriptor, pushedReplica *varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error)

	// Seal seals the log stream identified by the argument topicID and logStreamID.
	Seal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.SealResponse, error)

	// Unseal unseals the log stream identified by the argument topicID and logStreamID.
	Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*varlogpb.LogStreamDescriptor, error)

	// Sync copies logs of the log stream identified by the argument topicID and logStreamID
	// from the source storage node to the destination storage node.
	Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, srcStorageNodeID, dstStorageNodeID types.StorageNodeID) (*snpb.SyncStatus, error)

	// GetMRMembers returns metadata repositories of the cluster.
	GetMRMembers(ctx context.Context) (*vmspb.GetMRMembersResponse, error)

	// AddMRPeer registers a new metadata repository to the cluster.
	AddMRPeer(ctx context.Context, raftURL, rpcAddr string) (types.NodeID, error)

	// RemoveMRPeer unregisters the metadata repository from the cluster.
	RemoveMRPeer(ctx context.Context, raftURL string) error

	// GetStorageNodes returns a map of StorageNodeIDs and their addresses.
	GetStorageNodes(ctx context.Context) (map[types.StorageNodeID]string, error)

	// Trim deletes logs whose GLSNs are less than or equal to the argument lastGLSN.
	// Note that the return type of this method can be changed soon.
	Trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN) (map[types.LogStreamID]map[types.StorageNodeID]error, error)

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

func (c *admin) AddStorageNode(ctx context.Context, addr string) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	rsp, err := c.rpcClient.AddStorageNode(ctx, &vmspb.AddStorageNodeRequest{Address: addr})
	return rsp.GetStorageNode(), verrors.FromStatusError(err)
}

func (c *admin) UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error {
	_, err := c.rpcClient.UnregisterStorageNode(ctx, &vmspb.UnregisterStorageNodeRequest{StorageNodeID: storageNodeID})
	return verrors.FromStatusError(err)
}

func (c *admin) AddTopic(ctx context.Context) (varlogpb.TopicDescriptor, error) {
	rsp, err := c.rpcClient.AddTopic(ctx, &vmspb.AddTopicRequest{})
	if err != nil {
		return varlogpb.TopicDescriptor{}, verrors.FromStatusError(err)
	}
	return rsp.Topic, nil
}

func (c *admin) Topics(ctx context.Context) ([]varlogpb.TopicDescriptor, error) {
	rsp, err := c.rpcClient.Topics(ctx, &vmspb.TopicsRequest{})
	return rsp.GetTopics(), verrors.FromStatusError(err)
}

func (c *admin) DescribeTopic(ctx context.Context, topicID types.TopicID) (*vmspb.DescribeTopicResponse, error) {
	rsp, err := c.rpcClient.DescribeTopic(ctx, &vmspb.DescribeTopicRequest{TopicID: topicID})
	return rsp, verrors.FromStatusError(err)
}

func (c *admin) UnregisterTopic(ctx context.Context, topicID types.TopicID) (*vmspb.UnregisterTopicResponse, error) {
	rsp, err := c.rpcClient.UnregisterTopic(ctx, &vmspb.UnregisterTopicRequest{TopicID: topicID})
	return rsp, verrors.FromStatusError(err)
}

func (c *admin) AddLogStream(ctx context.Context, topicID types.TopicID, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.AddLogStream(ctx, &vmspb.AddLogStreamRequest{
		TopicID:  topicID,
		Replicas: logStreamReplicas,
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

func (c *admin) UpdateLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, poppedReplica, pushedReplica *varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.UpdateLogStream(ctx, &vmspb.UpdateLogStreamRequest{
		TopicID:       topicID,
		LogStreamID:   logStreamID,
		PoppedReplica: poppedReplica,
		PushedReplica: pushedReplica,
	})
	return rsp.GetLogStream(), verrors.FromStatusError(err)
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

func (c *admin) GetMRMembers(ctx context.Context) (*vmspb.GetMRMembersResponse, error) {
	rsp, err := c.rpcClient.GetMRMembers(ctx, &pbtypes.Empty{})
	return rsp, verrors.FromStatusError(err)
}

func (c *admin) AddMRPeer(ctx context.Context, raftURL, rpcAddr string) (types.NodeID, error) {
	rsp, err := c.rpcClient.AddMRPeer(ctx, &vmspb.AddMRPeerRequest{RaftURL: raftURL, RPCAddr: rpcAddr})
	return rsp.GetNodeID(), verrors.FromStatusError(err)
}

func (c *admin) RemoveMRPeer(ctx context.Context, raftURL string) error {
	_, err := c.rpcClient.RemoveMRPeer(ctx, &vmspb.RemoveMRPeerRequest{RaftURL: raftURL})
	return verrors.FromStatusError(err)
}

func (c *admin) GetStorageNodes(ctx context.Context) (map[types.StorageNodeID]string, error) {
	rsp, err := c.rpcClient.GetStorageNodes(ctx, &pbtypes.Empty{})
	return rsp.GetStorageNodes(), verrors.FromStatusError(err)
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
