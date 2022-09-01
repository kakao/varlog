package varlog

//go:generate mockgen -package varlog -destination admin_mock.go . Admin

import (
	"context"
	stderrors "errors"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"

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
	// It returns the ErrNotExist error if the storage node does not exist.
	GetStorageNode(ctx context.Context, snid types.StorageNodeID) (*vmspb.StorageNodeMetadata, error)
	// ListStorageNodes returns a list of storage node metadata.
	//
	// Note that it should return an empty slice rather than nil to encode
	// to an empty array in JSON if no storage node exists in the cluster.
	ListStorageNodes(ctx context.Context) ([]vmspb.StorageNodeMetadata, error)
	// GetStorageNodes returns a map of StorageNodeIDs and their addresses.
	// Deprecated: Use ListStorageNodes.
	GetStorageNodes(ctx context.Context) (map[types.StorageNodeID]vmspb.StorageNodeMetadata, error)
	// AddStorageNode registers a storage node, whose ID and address are
	// the argument snid and addr respectively, to the cluster.
	// It is okay to call AddStorageNode more than one time to add the same
	// storage node.
	// Once the storage node is registered, the pair of snid and addr
	// should not be changed.
	AddStorageNode(ctx context.Context, snid types.StorageNodeID, addr string) (*vmspb.StorageNodeMetadata, error)
	// UnregisterStorageNode unregisters a storage node identified by the
	// argument snid from the cluster.
	// It is okay to unregister not existed storage node.
	// If the storage node still has running log stream replicas, it
	// returns an error.
	UnregisterStorageNode(ctx context.Context, snid types.StorageNodeID) error

	// GetTopic returns the metadata of the topic specified by the argument
	// tpid.
	// It returns the ErrNotExist error if the topic does not exist.
	// If the admin could not fetch cluster metadata, it returns an error,
	// and users can retry this RPC.
	GetTopic(ctx context.Context, tpid types.TopicID) (*varlogpb.TopicDescriptor, error)
	// ListTopics returns a list of all topics in the cluster.
	//
	// Note that it should return an empty slice rather than nil to encode
	// to an empty array in JSON if no topic exists in the cluster.
	ListTopics(ctx context.Context) ([]varlogpb.TopicDescriptor, error)
	// AddTopic adds a new topic and returns its metadata including a
	// unique topid ID.
	// It returns an error if rejected by the metadata repository due to
	// redundant topic ID or something else, and users can retry this RPC.
	AddTopic(ctx context.Context) (*varlogpb.TopicDescriptor, error)
	// UnregisterTopic removes a topic identified by the argument tpid from
	// the cluster.
	// It is okay to delete not existed topic.
	// It returns an error if it tries to delete the topic which has active
	// log streams.
	// If the admin could not fetch cluster metadata, it returns an error,
	// and users can retry this RPC.
	UnregisterTopic(ctx context.Context, tpid types.TopicID) error

	// GetLogStream returns metadata of log stream specified by the argument tpid and lsid.
	// It returns an error if there is no topic or log stream.
	GetLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (*varlogpb.LogStreamDescriptor, error)
	// ListLogStreams returns a list of log streams belonging to the topic
	// tpid.
	//
	// Note that it should return an empty slice rather than nil to encode
	// to an empty array in JSON if no log stream exists in the topic.
	ListLogStreams(ctx context.Context, tpid types.TopicID) ([]varlogpb.LogStreamDescriptor, error)
	// DescribeTopic returns detailed metadata of the topic.
	// Deprecated: Use ListLogStreams.
	DescribeTopic(ctx context.Context, topicID types.TopicID) (*vmspb.DescribeTopicResponse, error)
	// AddLogStream adds a new log stream to the topic identified by the
	// argument tpid.
	// AddLogStream adds a new log stream to the topic tpid.
	// The admin chooses proper replicas if the argument replicas are
	// empty.
	// Otherwise, if the argument replicas are defined, the following
	// conditions should be satisfied:
	//
	//  - The number of replicas should be equal to the replication factor.
	//  - Each storage node for each replica should exist.
	//  - The log stream, which tries to add,  should not exist.
	//
	// Internally, it waits for the log stream for being sealed and
	// unsealed.
	AddLogStream(ctx context.Context, tpid types.TopicID, replicas []*varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error)
	// UpdateLogStream changes replicas of the log stream.
	// This method swaps two replicas - the argument poppedReplica and
	// pushedReplica. The poppedReplica is the old replica that belonged to
	// the log stream, however, pushedReplica is the new replica to be
	// added to the log stream.
	UpdateLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, poppedReplica varlogpb.ReplicaDescriptor, pushedReplica varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error)
	// UnregisterLogStream unregisters a log stream from the cluster.
	UnregisterLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) error

	// RemoveLogStreamReplica removes a log stream replica from the storage
	// node.
	RemoveLogStreamReplica(ctx context.Context, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID) error

	// Seal seals the log stream identified by the argument tpid and lsid.
	Seal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (*vmspb.SealResponse, error)
	// Unseal unseals the log stream identified by the argument tpid and
	// lsid.
	Unseal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (*varlogpb.LogStreamDescriptor, error)
	// Sync copies logs of the log stream identified by the argument tpid
	// and lsid from the source storage node to the destination storage
	// node.
	Sync(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, srcid, dstid types.StorageNodeID) (*snpb.SyncStatus, error)
	// Trim deletes logs whose GLSNs are less than or equal to the argument
	// lastGLSN.
	// Note that the return type of this method can be changed soon.
	Trim(ctx context.Context, tpid types.TopicID, lastGLSN types.GLSN) (map[types.LogStreamID]map[types.StorageNodeID]error, error)

	GetMetadataRepositoryNode(ctx context.Context, nid types.NodeID) (*varlogpb.MetadataRepositoryNode, error)
	ListMetadataRepositoryNodes(ctx context.Context) ([]varlogpb.MetadataRepositoryNode, error)
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

func (c *admin) GetStorageNode(ctx context.Context, snid types.StorageNodeID) (*vmspb.StorageNodeMetadata, error) {
	rsp, err := c.rpcClient.GetStorageNode(ctx, &vmspb.GetStorageNodeRequest{
		StorageNodeID: snid,
	})
	if err != nil {
		if st := status.Convert(err); st.Code() == codes.NotFound {
			err = verrors.ErrNotExist
		}
		return nil, errors.WithMessage(err, "admin: get storage node")
	}
	return rsp.StorageNode, nil
}

func (c *admin) ListStorageNodes(ctx context.Context) ([]vmspb.StorageNodeMetadata, error) {
	rsp, err := c.rpcClient.ListStorageNodes(ctx, &vmspb.ListStorageNodesRequest{})
	if err != nil {
		return nil, errors.WithMessage(err, "admin: list storage nodes") //verrors.FromStatusError(err)
	}

	if len(rsp.StorageNodes) > 0 {
		return rsp.StorageNodes, nil
	}
	return []vmspb.StorageNodeMetadata{}, nil
}

func (c *admin) GetStorageNodes(ctx context.Context) (map[types.StorageNodeID]vmspb.StorageNodeMetadata, error) {
	snms, err := c.ListStorageNodes(ctx)
	if err != nil {
		return nil, err
	}
	ret := make(map[types.StorageNodeID]vmspb.StorageNodeMetadata, len(snms))
	for _, snm := range snms {
		ret[snm.StorageNode.StorageNodeID] = snm
	}
	return ret, nil
}

func (c *admin) AddStorageNode(ctx context.Context, snid types.StorageNodeID, addr string) (*vmspb.StorageNodeMetadata, error) {
	rsp, err := c.rpcClient.AddStorageNode(ctx, &vmspb.AddStorageNodeRequest{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: snid,
			Address:       addr,
		},
	})
	return rsp.GetStorageNode(), err
}

func (c *admin) UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error {
	_, err := c.rpcClient.UnregisterStorageNode(ctx, &vmspb.UnregisterStorageNodeRequest{StorageNodeID: storageNodeID})
	return err
}

func (c *admin) GetTopic(ctx context.Context, tpid types.TopicID) (*varlogpb.TopicDescriptor, error) {
	rsp, err := c.rpcClient.GetTopic(ctx, &vmspb.GetTopicRequest{
		TopicID: tpid,
	})
	if err != nil {
		if st := status.Convert(err); st.Code() == codes.NotFound {
			err = verrors.ErrNotExist
		}
		return nil, errors.WithMessage(err, "admin: get topic")
	}
	return rsp.GetTopic(), nil
}

func (c *admin) ListTopics(ctx context.Context) ([]varlogpb.TopicDescriptor, error) {
	rsp, err := c.rpcClient.ListTopics(ctx, &vmspb.ListTopicsRequest{})
	if err != nil {
		return nil, errors.WithMessage(err, "admin: list topics")
	}

	if len(rsp.Topics) > 0 {
		return rsp.Topics, nil
	}
	return []varlogpb.TopicDescriptor{}, nil
}

func (c *admin) AddTopic(ctx context.Context) (*varlogpb.TopicDescriptor, error) {
	rsp, err := c.rpcClient.AddTopic(ctx, &vmspb.AddTopicRequest{})
	if err != nil {
		return nil, err
	}
	return rsp.Topic, nil
}

func (c *admin) UnregisterTopic(ctx context.Context, topicID types.TopicID) error {
	_, err := c.rpcClient.UnregisterTopic(ctx, &vmspb.UnregisterTopicRequest{TopicID: topicID})
	return err
}

func (c *admin) GetLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.GetLogStream(ctx, &vmspb.GetLogStreamRequest{
		TopicID:     tpid,
		LogStreamID: lsid,
	})
	if err != nil {
		if st := status.Convert(err); st.Code() == codes.NotFound {
			err = verrors.ErrNotExist
		}
		return nil, errors.WithMessage(err, "admin: get log stream")
	}
	return rsp.GetLogStream(), nil
}

func (c *admin) ListLogStreams(ctx context.Context, tpid types.TopicID) ([]varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.ListLogStreams(ctx, &vmspb.ListLogStreamsRequest{
		TopicID: tpid,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "admin: list log streams")
	}

	if len(rsp.LogStreams) > 0 {
		return rsp.LogStreams, nil
	}
	return []varlogpb.LogStreamDescriptor{}, nil
}

func (c *admin) DescribeTopic(ctx context.Context, topicID types.TopicID) (*vmspb.DescribeTopicResponse, error) {
	rsp, err := c.rpcClient.DescribeTopic(ctx, &vmspb.DescribeTopicRequest{TopicID: topicID})
	return rsp, err
}

func (c *admin) AddLogStream(ctx context.Context, topicID types.TopicID, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.AddLogStream(ctx, &vmspb.AddLogStreamRequest{
		TopicID:  topicID,
		Replicas: logStreamReplicas,
	})
	return rsp.GetLogStream(), err
}

func (c *admin) UpdateLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, poppedReplica, pushedReplica varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.UpdateLogStream(ctx, &vmspb.UpdateLogStreamRequest{
		TopicID:       topicID,
		LogStreamID:   logStreamID,
		PoppedReplica: poppedReplica,
		PushedReplica: pushedReplica,
	})
	return rsp.GetLogStream(), err
}

func (c *admin) UnregisterLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) error {
	_, err := c.rpcClient.UnregisterLogStream(ctx, &vmspb.UnregisterLogStreamRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
	})
	return err
}

func (c *admin) RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID) error {
	_, err := c.rpcClient.RemoveLogStreamReplica(ctx, &vmspb.RemoveLogStreamReplicaRequest{
		StorageNodeID: storageNodeID,
		TopicID:       topicID,
		LogStreamID:   logStreamID,
	})
	return err
}

func (c *admin) Seal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.SealResponse, error) {
	rsp, err := c.rpcClient.Seal(ctx, &vmspb.SealRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
	})
	return rsp, err
}

func (c *admin) Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.Unseal(ctx, &vmspb.UnsealRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
	})
	return rsp.GetLogStream(), err
}

func (c *admin) Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, srcStorageNodeID, dstStorageNodeID types.StorageNodeID) (*snpb.SyncStatus, error) {
	rsp, err := c.rpcClient.Sync(ctx, &vmspb.SyncRequest{
		TopicID:          topicID,
		LogStreamID:      logStreamID,
		SrcStorageNodeID: srcStorageNodeID,
		DstStorageNodeID: dstStorageNodeID,
	})
	return rsp.GetStatus(), err
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
			err = stderrors.New(result.Error)
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

func (c *admin) ListMetadataRepositoryNodes(ctx context.Context) ([]varlogpb.MetadataRepositoryNode, error) {
	rsp, err := c.rpcClient.ListMetadataRepositoryNodes(ctx, &vmspb.ListMetadataRepositoryNodesRequest{})
	if err != nil {
		return nil, errors.WithMessage(err, "admin: list metadata repositories")
	}

	if len(rsp.Nodes) > 0 {
		return rsp.Nodes, nil
	}
	return []varlogpb.MetadataRepositoryNode{}, nil
}

func (c *admin) GetMRMembers(ctx context.Context) (*vmspb.GetMRMembersResponse, error) {
	rsp, err := c.rpcClient.GetMRMembers(ctx, &pbtypes.Empty{})
	return rsp, err
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
	return rsp.GetNodeID(), err
}

func (c *admin) DeleteMetadataRepositoryNode(ctx context.Context, nid types.NodeID) error {
	_, err := c.rpcClient.DeleteMetadataRepositoryNode(ctx, &vmspb.DeleteMetadataRepositoryNodeRequest{
		NodeID: nid,
	})
	return err
}

func (c *admin) RemoveMRPeer(ctx context.Context, raftURL string) error {
	_, err := c.rpcClient.RemoveMRPeer(ctx, &vmspb.RemoveMRPeerRequest{RaftURL: raftURL})
	return err
}
