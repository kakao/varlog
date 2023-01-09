package admin

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/admpb"
)

type server struct {
	admin *Admin
}

var _ admpb.ClusterManagerServer = (*server)(nil)

func (s *server) GetStorageNode(ctx context.Context, req *admpb.GetStorageNodeRequest) (*admpb.GetStorageNodeResponse, error) {
	snm, err := s.admin.getStorageNode(ctx, req.StorageNodeID)
	if err != nil {
		return nil, err
	}
	return &admpb.GetStorageNodeResponse{StorageNode: snm}, nil
}

func (s *server) ListStorageNodes(ctx context.Context, _ *admpb.ListStorageNodesRequest) (*admpb.ListStorageNodesResponse, error) {
	snms, err := s.admin.listStorageNodes(ctx)
	if err != nil {
		return nil, err
	}
	return &admpb.ListStorageNodesResponse{StorageNodes: snms}, nil
}

func (s *server) AddStorageNode(ctx context.Context, req *admpb.AddStorageNodeRequest) (*admpb.AddStorageNodeResponse, error) {
	snm, err := s.admin.addStorageNode(ctx, req.StorageNode.StorageNodeID, req.StorageNode.Address)
	if err != nil {
		return nil, err
	}
	return &admpb.AddStorageNodeResponse{StorageNode: snm}, nil
}

func (s *server) UnregisterStorageNode(ctx context.Context, req *admpb.UnregisterStorageNodeRequest) (*admpb.UnregisterStorageNodeResponse, error) {
	err := s.admin.unregisterStorageNode(ctx, req.GetStorageNodeID())
	if err != nil {
		return nil, err
	}
	return &admpb.UnregisterStorageNodeResponse{}, nil
}

func (s *server) GetTopic(ctx context.Context, req *admpb.GetTopicRequest) (*admpb.GetTopicResponse, error) {
	td, err := s.admin.getTopic(ctx, req.TopicID)
	if err != nil {
		return nil, err
	}
	return &admpb.GetTopicResponse{Topic: td}, nil
}

func (s *server) ListTopics(ctx context.Context, req *admpb.ListTopicsRequest) (*admpb.ListTopicsResponse, error) {
	tds, err := s.admin.listTopics(ctx)
	if err != nil {
		return nil, err
	}
	return &admpb.ListTopicsResponse{Topics: tds}, nil
}

func (s *server) AddTopic(ctx context.Context, req *admpb.AddTopicRequest) (*admpb.AddTopicResponse, error) {
	td, err := s.admin.addTopic(ctx)
	if err != nil {
		return nil, err
	}
	return &admpb.AddTopicResponse{Topic: td}, err
}

func (s *server) UnregisterTopic(ctx context.Context, req *admpb.UnregisterTopicRequest) (*admpb.UnregisterTopicResponse, error) {
	err := s.admin.unregisterTopic(ctx, req.GetTopicID())
	if err != nil {
		return nil, err
	}
	return &admpb.UnregisterTopicResponse{}, nil
}

func (s *server) GetLogStream(ctx context.Context, req *admpb.GetLogStreamRequest) (*admpb.GetLogStreamResponse, error) {
	lsd, err := s.admin.getLogStream(ctx, req.TopicID, req.LogStreamID)
	return &admpb.GetLogStreamResponse{LogStream: lsd}, err
}

func (s *server) ListLogStreams(ctx context.Context, req *admpb.ListLogStreamsRequest) (*admpb.ListLogStreamsResponse, error) {
	lsds, err := s.admin.listLogStreams(ctx, req.TopicID)
	return &admpb.ListLogStreamsResponse{LogStreams: lsds}, err
}

func (s *server) DescribeTopic(ctx context.Context, req *admpb.DescribeTopicRequest) (*admpb.DescribeTopicResponse, error) {
	td, lsds, err := s.admin.describeTopic(ctx, req.TopicID)
	if err != nil {
		return nil, verrors.ToStatusErrorWithCode(err, codes.Unavailable)
	}
	return &admpb.DescribeTopicResponse{
		Topic:      td,
		LogStreams: lsds,
	}, nil
}

func (s *server) AddLogStream(ctx context.Context, req *admpb.AddLogStreamRequest) (*admpb.AddLogStreamResponse, error) {
	logStreamDesc, err := s.admin.addLogStream(ctx, req.GetTopicID(), req.GetReplicas())
	if err != nil {
		code := status.Code(err)
		if code != codes.ResourceExhausted {
			code = codes.Unavailable
		}
		return nil, verrors.ToStatusErrorWithCode(err, code)
	}
	return &admpb.AddLogStreamResponse{LogStream: logStreamDesc}, nil
}

func (s *server) UnregisterLogStream(ctx context.Context, req *admpb.UnregisterLogStreamRequest) (*admpb.UnregisterLogStreamResponse, error) {
	err := s.admin.unregisterLogStream(ctx, req.GetTopicID(), req.GetLogStreamID())
	return &admpb.UnregisterLogStreamResponse{}, verrors.ToStatusError(err)
}

func (s *server) RemoveLogStreamReplica(ctx context.Context, req *admpb.RemoveLogStreamReplicaRequest) (*admpb.RemoveLogStreamReplicaResponse, error) {
	err := s.admin.removeLogStreamReplica(ctx, req.GetStorageNodeID(), req.GetTopicID(), req.GetLogStreamID())
	return &admpb.RemoveLogStreamReplicaResponse{}, verrors.ToStatusError(err)
}

func (s *server) UpdateLogStream(ctx context.Context, req *admpb.UpdateLogStreamRequest) (*admpb.UpdateLogStreamResponse, error) {
	lsdesc, err := s.admin.updateLogStream(ctx, req.GetLogStreamID(), req.PoppedReplica, req.PushedReplica)
	return &admpb.UpdateLogStreamResponse{LogStream: lsdesc}, verrors.ToStatusError(err)
}

func (s *server) Seal(ctx context.Context, req *admpb.SealRequest) (*admpb.SealResponse, error) {
	lsmetas, sealedGLSN, err := s.admin.seal(ctx, req.GetTopicID(), req.GetLogStreamID())
	return &admpb.SealResponse{
		LogStreams: lsmetas,
		SealedGLSN: sealedGLSN,
	}, verrors.ToStatusError(err)
}

func (s *server) Sync(ctx context.Context, req *admpb.SyncRequest) (*admpb.SyncResponse, error) {
	status, err := s.admin.sync(ctx, req.GetTopicID(), req.GetLogStreamID(), req.GetSrcStorageNodeID(), req.GetDstStorageNodeID())
	return &admpb.SyncResponse{Status: status}, verrors.ToStatusError(err)
}

func (s *server) Unseal(ctx context.Context, req *admpb.UnsealRequest) (*admpb.UnsealResponse, error) {
	lsdesc, err := s.admin.unseal(ctx, req.GetTopicID(), req.GetLogStreamID())
	return &admpb.UnsealResponse{LogStream: lsdesc}, verrors.ToStatusError(err)
}

func (s *server) GetMetadataRepositoryNode(ctx context.Context, req *admpb.GetMetadataRepositoryNodeRequest) (*admpb.GetMetadataRepositoryNodeResponse, error) {
	node, err := s.admin.getMetadataRepositoryNode(ctx, req.NodeID)
	return &admpb.GetMetadataRepositoryNodeResponse{Node: node}, err
}

func (s *server) ListMetadataRepositoryNodes(ctx context.Context, _ *admpb.ListMetadataRepositoryNodesRequest) (*admpb.ListMetadataRepositoryNodesResponse, error) {
	nodes, err := s.admin.listMetadataRepositoryNodes(ctx)
	return &admpb.ListMetadataRepositoryNodesResponse{Nodes: nodes}, err
}

func (s *server) GetMRMembers(ctx context.Context, req *pbtypes.Empty) (*admpb.GetMRMembersResponse, error) {
	var rsp *admpb.GetMRMembersResponse
	mrInfo, err := s.admin.mrInfos(ctx)
	if err != nil {
		return rsp, verrors.ToStatusError(err)
	}
	rsp = &admpb.GetMRMembersResponse{
		Leader:            mrInfo.Leader,
		ReplicationFactor: mrInfo.ReplicationFactor,
		Members:           make(map[types.NodeID]string, len(mrInfo.Members)),
	}
	for nodeID, m := range mrInfo.Members {
		rsp.Members[nodeID] = m.Peer
	}
	return rsp, nil
}

func (s *server) AddMetadataRepositoryNode(ctx context.Context, req *admpb.AddMetadataRepositoryNodeRequest) (*admpb.AddMetadataRepositoryNodeResponse, error) {
	node, err := s.admin.addMetadataRepositoryNode(ctx, req.RaftURL, req.RPCAddr)
	return &admpb.AddMetadataRepositoryNodeResponse{Node: node}, err
}

func (s *server) AddMRPeer(ctx context.Context, req *admpb.AddMRPeerRequest) (*admpb.AddMRPeerResponse, error) {
	nodeID, err := s.admin.addMRPeer(ctx, req.RaftURL, req.RPCAddr)
	return &admpb.AddMRPeerResponse{NodeID: nodeID}, verrors.ToStatusError(err)
}

func (s *server) DeleteMetadataRepositoryNode(ctx context.Context, req *admpb.DeleteMetadataRepositoryNodeRequest) (*admpb.DeleteMetadataRepositoryNodeResponse, error) {
	err := s.admin.deleteMetadataRepositoryNode(ctx, req.NodeID)
	return &admpb.DeleteMetadataRepositoryNodeResponse{}, err
}

func (s *server) RemoveMRPeer(ctx context.Context, req *admpb.RemoveMRPeerRequest) (*admpb.RemoveMRPeerResponse, error) {
	err := s.admin.removeMRPeer(ctx, req.RaftURL)
	return &admpb.RemoveMRPeerResponse{}, verrors.ToStatusError(err)
}

func (s *server) Trim(ctx context.Context, req *admpb.TrimRequest) (*admpb.TrimResponse, error) {
	res, err := s.admin.trim(ctx, req.TopicID, req.LastGLSN)
	return &admpb.TrimResponse{Results: res}, verrors.ToStatusError(err)
}
