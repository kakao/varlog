package admin

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/vmspb"
)

type server struct {
	admin *Admin
}

var _ vmspb.ClusterManagerServer = (*server)(nil)

func (s *server) GetStorageNode(ctx context.Context, req *vmspb.GetStorageNodeRequest) (*vmspb.GetStorageNodeResponse, error) {
	snm, err := s.admin.getStorageNode(ctx, req.StorageNodeID)
	if err != nil {
		return nil, err
	}
	return &vmspb.GetStorageNodeResponse{StorageNode: snm}, nil
}

func (s *server) ListStorageNodes(ctx context.Context, _ *vmspb.ListStorageNodesRequest) (*vmspb.ListStorageNodesResponse, error) {
	snms, err := s.admin.listStorageNodes(ctx)
	if err != nil {
		return nil, err
	}
	return &vmspb.ListStorageNodesResponse{StorageNodes: snms}, nil
}

func (s *server) AddStorageNode(ctx context.Context, req *vmspb.AddStorageNodeRequest) (*vmspb.AddStorageNodeResponse, error) {
	snm, err := s.admin.addStorageNode(ctx, req.StorageNode.StorageNodeID, req.StorageNode.Address)
	if err != nil {
		return nil, err
	}
	return &vmspb.AddStorageNodeResponse{StorageNode: snm}, nil
}

func (s *server) UnregisterStorageNode(ctx context.Context, req *vmspb.UnregisterStorageNodeRequest) (*vmspb.UnregisterStorageNodeResponse, error) {
	err := s.admin.unregisterStorageNode(ctx, req.GetStorageNodeID())
	if err != nil {
		return nil, err
	}
	return &vmspb.UnregisterStorageNodeResponse{}, nil
}

func (s *server) GetTopic(ctx context.Context, req *vmspb.GetTopicRequest) (*vmspb.GetTopicResponse, error) {
	td, err := s.admin.getTopic(ctx, req.TopicID)
	if err != nil {
		return nil, err
	}
	return &vmspb.GetTopicResponse{Topic: td}, nil
}

func (s *server) ListTopics(ctx context.Context, req *vmspb.ListTopicsRequest) (*vmspb.ListTopicsResponse, error) {
	tds, err := s.admin.listTopics(ctx)
	if err != nil {
		return nil, err
	}
	return &vmspb.ListTopicsResponse{Topics: tds}, nil
}

func (s *server) AddTopic(ctx context.Context, req *vmspb.AddTopicRequest) (*vmspb.AddTopicResponse, error) {
	td, err := s.admin.addTopic(ctx)
	if err != nil {
		return nil, err
	}
	return &vmspb.AddTopicResponse{Topic: td}, err
}

func (s *server) UnregisterTopic(ctx context.Context, req *vmspb.UnregisterTopicRequest) (*vmspb.UnregisterTopicResponse, error) {
	err := s.admin.unregisterTopic(ctx, req.GetTopicID())
	if err != nil {
		return nil, err
	}
	return &vmspb.UnregisterTopicResponse{}, nil
}

func (s *server) GetLogStream(ctx context.Context, req *vmspb.GetLogStreamRequest) (*vmspb.GetLogStreamResponse, error) {
	lsd, err := s.admin.getLogStream(ctx, req.TopicID, req.LogStreamID)
	return &vmspb.GetLogStreamResponse{LogStream: lsd}, err
}

func (s *server) ListLogStreams(ctx context.Context, req *vmspb.ListLogStreamsRequest) (*vmspb.ListLogStreamsResponse, error) {
	lsds, err := s.admin.listLogStreams(ctx, req.TopicID)
	return &vmspb.ListLogStreamsResponse{LogStreams: lsds}, err
}

func (s *server) DescribeTopic(ctx context.Context, req *vmspb.DescribeTopicRequest) (*vmspb.DescribeTopicResponse, error) {
	td, lsds, err := s.admin.describeTopic(ctx, req.TopicID)
	if err != nil {
		return nil, verrors.ToStatusErrorWithCode(err, codes.Unavailable)
	}
	return &vmspb.DescribeTopicResponse{
		Topic:      td,
		LogStreams: lsds,
	}, nil
}

func (s *server) AddLogStream(ctx context.Context, req *vmspb.AddLogStreamRequest) (*vmspb.AddLogStreamResponse, error) {
	logStreamDesc, err := s.admin.addLogStream(ctx, req.GetTopicID(), req.GetReplicas())
	if err != nil {
		return nil, verrors.ToStatusErrorWithCode(err, codes.Unavailable)
	}
	return &vmspb.AddLogStreamResponse{LogStream: logStreamDesc}, nil
}

func (s *server) UnregisterLogStream(ctx context.Context, req *vmspb.UnregisterLogStreamRequest) (*vmspb.UnregisterLogStreamResponse, error) {
	err := s.admin.unregisterLogStream(ctx, req.GetTopicID(), req.GetLogStreamID())
	return &vmspb.UnregisterLogStreamResponse{}, verrors.ToStatusError(err)
}

func (s *server) RemoveLogStreamReplica(ctx context.Context, req *vmspb.RemoveLogStreamReplicaRequest) (*vmspb.RemoveLogStreamReplicaResponse, error) {
	err := s.admin.removeLogStreamReplica(ctx, req.GetStorageNodeID(), req.GetTopicID(), req.GetLogStreamID())
	return &vmspb.RemoveLogStreamReplicaResponse{}, verrors.ToStatusError(err)
}

func (s *server) UpdateLogStream(ctx context.Context, req *vmspb.UpdateLogStreamRequest) (*vmspb.UpdateLogStreamResponse, error) {
	lsdesc, err := s.admin.updateLogStream(ctx, req.GetLogStreamID(), req.GetPoppedReplica(), req.GetPushedReplica())
	return &vmspb.UpdateLogStreamResponse{LogStream: lsdesc}, verrors.ToStatusError(err)
}

func (s *server) Seal(ctx context.Context, req *vmspb.SealRequest) (*vmspb.SealResponse, error) {
	lsmetas, sealedGLSN, err := s.admin.seal(ctx, req.GetTopicID(), req.GetLogStreamID())
	return &vmspb.SealResponse{
		LogStreams: lsmetas,
		SealedGLSN: sealedGLSN,
	}, verrors.ToStatusError(err)
}

func (s *server) Sync(ctx context.Context, req *vmspb.SyncRequest) (*vmspb.SyncResponse, error) {
	status, err := s.admin.sync(ctx, req.GetTopicID(), req.GetLogStreamID(), req.GetSrcStorageNodeID(), req.GetDstStorageNodeID())
	return &vmspb.SyncResponse{Status: status}, verrors.ToStatusError(err)
}

func (s *server) Unseal(ctx context.Context, req *vmspb.UnsealRequest) (*vmspb.UnsealResponse, error) {
	lsdesc, err := s.admin.unseal(ctx, req.GetTopicID(), req.GetLogStreamID())
	return &vmspb.UnsealResponse{LogStream: lsdesc}, verrors.ToStatusError(err)
}

func (s *server) GetMetadataRepositoryNode(ctx context.Context, req *vmspb.GetMetadataRepositoryNodeRequest) (*vmspb.GetMetadataRepositoryNodeResponse, error) {
	node, err := s.admin.getMetadataRepositoryNode(ctx, req.NodeID)
	return &vmspb.GetMetadataRepositoryNodeResponse{Node: node}, err
}

func (s *server) ListMetadataRepositoryNodes(ctx context.Context, _ *vmspb.ListMetadataRepositoryNodesRequest) (*vmspb.ListMetadataRepositoryNodesResponse, error) {
	nodes, err := s.admin.listMetadataRepositoryNodes(ctx)
	return &vmspb.ListMetadataRepositoryNodesResponse{Nodes: nodes}, err
}

func (s *server) GetMRMembers(ctx context.Context, req *pbtypes.Empty) (*vmspb.GetMRMembersResponse, error) {
	var rsp *vmspb.GetMRMembersResponse
	mrInfo, err := s.admin.mrInfos(ctx)
	if err != nil {
		return rsp, verrors.ToStatusError(err)
	}
	rsp = &vmspb.GetMRMembersResponse{
		Leader:            mrInfo.Leader,
		ReplicationFactor: mrInfo.ReplicationFactor,
		Members:           make(map[types.NodeID]string, len(mrInfo.Members)),
	}
	for nodeID, m := range mrInfo.Members {
		rsp.Members[nodeID] = m.Peer
	}
	return rsp, nil
}

func (s *server) AddMetadataRepositoryNode(ctx context.Context, req *vmspb.AddMetadataRepositoryNodeRequest) (*vmspb.AddMetadataRepositoryNodeResponse, error) {
	node, err := s.admin.addMetadataRepositoryNode(ctx, req.RaftURL, req.RPCAddr)
	return &vmspb.AddMetadataRepositoryNodeResponse{Node: node}, err
}

func (s *server) AddMRPeer(ctx context.Context, req *vmspb.AddMRPeerRequest) (*vmspb.AddMRPeerResponse, error) {
	nodeID, err := s.admin.addMRPeer(ctx, req.RaftURL, req.RPCAddr)
	return &vmspb.AddMRPeerResponse{NodeID: nodeID}, verrors.ToStatusError(err)
}

func (s *server) DeleteMetadataRepositoryNode(ctx context.Context, req *vmspb.DeleteMetadataRepositoryNodeRequest) (*vmspb.DeleteMetadataRepositoryNodeResponse, error) {
	err := s.admin.deleteMetadataRepositoryNode(ctx, req.NodeID)
	return &vmspb.DeleteMetadataRepositoryNodeResponse{}, err
}

func (s *server) RemoveMRPeer(ctx context.Context, req *vmspb.RemoveMRPeerRequest) (*vmspb.RemoveMRPeerResponse, error) {
	err := s.admin.removeMRPeer(ctx, req.RaftURL)
	return &vmspb.RemoveMRPeerResponse{}, verrors.ToStatusError(err)
}

func (s *server) Trim(ctx context.Context, req *vmspb.TrimRequest) (*vmspb.TrimResponse, error) {
	res, err := s.admin.trim(ctx, req.TopicID, req.LastGLSN)
	return &vmspb.TrimResponse{Results: res}, verrors.ToStatusError(err)
}
