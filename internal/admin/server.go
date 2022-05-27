package admin

import (
	"context"
	"fmt"

	pbtypes "github.com/gogo/protobuf/types"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
)

type server struct {
	admin *Admin
}

var _ vmspb.ClusterManagerServer = (*server)(nil)

type handler func(ctx context.Context, req interface{}) (rsp interface{}, err error)

func (s *server) withTelemetry(ctx context.Context, spanName string, req interface{}, fn handler) (rsp interface{}, err error) {
	rsp, err = fn(ctx, req)
	if err == nil {
		s.admin.logger.Debug(spanName, zap.Stringer("request", req.(fmt.Stringer)), zap.Stringer("response", rsp.(fmt.Stringer)))
	} else {
		s.admin.logger.Error(spanName, zap.Stringer("request", req.(fmt.Stringer)), zap.Error(err))
	}
	return rsp, err
}

func (s *server) AddStorageNode(ctx context.Context, req *vmspb.AddStorageNodeRequest) (*vmspb.AddStorageNodeResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/AddStorageNode", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			snmeta, err := s.admin.addStorageNode(ctx, req.StorageNode.StorageNodeID, req.StorageNode.Address)
			return &vmspb.AddStorageNodeResponse{StorageNode: snmeta}, err
		},
	)
	return rspI.(*vmspb.AddStorageNodeResponse), verrors.ToStatusError(err)
}

func (s *server) UnregisterStorageNode(ctx context.Context, req *vmspb.UnregisterStorageNodeRequest) (*vmspb.UnregisterStorageNodeResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/UnregisterStorageNode", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.admin.unregisterStorageNode(ctx, req.GetStorageNodeID())
			return &vmspb.UnregisterStorageNodeResponse{}, err
		},
	)
	return rspI.(*vmspb.UnregisterStorageNodeResponse), verrors.ToStatusError(err)
}

func (s *server) AddTopic(ctx context.Context, req *vmspb.AddTopicRequest) (*vmspb.AddTopicResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/AddTopic", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			topicDesc, err := s.admin.addTopic(ctx)
			return &vmspb.AddTopicResponse{Topic: topicDesc}, err
		},
	)
	return rspI.(*vmspb.AddTopicResponse), verrors.ToStatusErrorWithCode(err, codes.Unavailable)
}

func (s *server) Topics(ctx context.Context, req *vmspb.TopicsRequest) (*vmspb.TopicsResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/Topics", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			tds, err := s.admin.topics(ctx)
			return &vmspb.TopicsResponse{Topics: tds}, err
		},
	)
	return rspI.(*vmspb.TopicsResponse), verrors.ToStatusErrorWithCode(err, codes.Unavailable)
}

func (s *server) DescribeTopic(ctx context.Context, req *vmspb.DescribeTopicRequest) (*vmspb.DescribeTopicResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/DescribeTopic", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			td, lsds, err := s.admin.describeTopic(ctx, req.TopicID)
			return &vmspb.DescribeTopicResponse{
				Topic:      td,
				LogStreams: lsds,
			}, err
		},
	)
	return rspI.(*vmspb.DescribeTopicResponse), verrors.ToStatusErrorWithCode(err, codes.Unavailable)
}

func (s *server) UnregisterTopic(ctx context.Context, req *vmspb.UnregisterTopicRequest) (*vmspb.UnregisterTopicResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/UnregisterTopic", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.admin.unregisterTopic(ctx, req.GetTopicID())
			return &vmspb.UnregisterTopicResponse{}, err
		},
	)
	return rspI.(*vmspb.UnregisterTopicResponse), verrors.ToStatusError(err)
}

func (s *server) AddLogStream(ctx context.Context, req *vmspb.AddLogStreamRequest) (*vmspb.AddLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/AddLogStream", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			logStreamDesc, err := s.admin.AddLogStream(ctx, req.GetTopicID(), req.GetReplicas())
			return &vmspb.AddLogStreamResponse{LogStream: logStreamDesc}, err
		},
	)
	return rspI.(*vmspb.AddLogStreamResponse), verrors.ToStatusErrorWithCode(err, codes.Unavailable)
}

func (s *server) UnregisterLogStream(ctx context.Context, req *vmspb.UnregisterLogStreamRequest) (*vmspb.UnregisterLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/UnregisterLogStream", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.admin.unregisterLogStream(ctx, req.GetTopicID(), req.GetLogStreamID())
			return &vmspb.UnregisterLogStreamResponse{}, err
		},
	)
	return rspI.(*vmspb.UnregisterLogStreamResponse), verrors.ToStatusError(err)
}

func (s *server) RemoveLogStreamReplica(ctx context.Context, req *vmspb.RemoveLogStreamReplicaRequest) (*vmspb.RemoveLogStreamReplicaResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/RemoveLogStreamReplica", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.admin.removeLogStreamReplica(ctx, req.GetStorageNodeID(), req.GetTopicID(), req.GetLogStreamID())
			return &vmspb.RemoveLogStreamReplicaResponse{}, err
		},
	)
	return rspI.(*vmspb.RemoveLogStreamReplicaResponse), verrors.ToStatusError(err)
}

func (s *server) UpdateLogStream(ctx context.Context, req *vmspb.UpdateLogStreamRequest) (*vmspb.UpdateLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/UpdateLogStream", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			lsdesc, err := s.admin.updateLogStream(ctx, req.GetLogStreamID(), req.GetPoppedReplica(), req.GetPushedReplica())
			return &vmspb.UpdateLogStreamResponse{LogStream: lsdesc}, err
		},
	)
	return rspI.(*vmspb.UpdateLogStreamResponse), verrors.ToStatusError(err)
}

func (s *server) Seal(ctx context.Context, req *vmspb.SealRequest) (*vmspb.SealResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/Seal", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			lsmetas, sealedGLSN, err := s.admin.Seal(ctx, req.GetTopicID(), req.GetLogStreamID())
			return &vmspb.SealResponse{
				LogStreams: lsmetas,
				SealedGLSN: sealedGLSN,
			}, err
		},
	)
	return rspI.(*vmspb.SealResponse), verrors.ToStatusError(err)
}

func (s *server) Sync(ctx context.Context, req *vmspb.SyncRequest) (*vmspb.SyncResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/Sync", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			status, err := s.admin.Sync(ctx, req.GetTopicID(), req.GetLogStreamID(), req.GetSrcStorageNodeID(), req.GetDstStorageNodeID())
			return &vmspb.SyncResponse{Status: status}, err
		},
	)
	return rspI.(*vmspb.SyncResponse), verrors.ToStatusError(err)
}

func (s *server) Unseal(ctx context.Context, req *vmspb.UnsealRequest) (*vmspb.UnsealResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/Unseal", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			lsdesc, err := s.admin.Unseal(ctx, req.GetTopicID(), req.GetLogStreamID())
			return &vmspb.UnsealResponse{LogStream: lsdesc}, err
		},
	)
	return rspI.(*vmspb.UnsealResponse), verrors.ToStatusError(err)
}

func (s *server) GetMRMembers(ctx context.Context, req *pbtypes.Empty) (*vmspb.GetMRMembersResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/GetMRMembers", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			var rsp *vmspb.GetMRMembersResponse
			mrInfo, err := s.admin.mrInfos(ctx)
			if err != nil {
				return rsp, err
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
		},
	)
	return rspI.(*vmspb.GetMRMembersResponse), verrors.ToStatusError(err)
}

func (s *server) AddMRPeer(ctx context.Context, req *vmspb.AddMRPeerRequest) (*vmspb.AddMRPeerResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/AddMRPeer", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			nodeID, err := s.admin.addMRPeer(ctx, req.RaftURL, req.RPCAddr)
			return &vmspb.AddMRPeerResponse{NodeID: nodeID}, err
		},
	)
	return rspI.(*vmspb.AddMRPeerResponse), verrors.ToStatusError(err)
}

func (s *server) RemoveMRPeer(ctx context.Context, req *vmspb.RemoveMRPeerRequest) (*vmspb.RemoveMRPeerResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/RemoveMRPeer", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.admin.removeMRPeer(ctx, req.RaftURL)
			return &vmspb.RemoveMRPeerResponse{}, err
		},
	)
	return rspI.(*vmspb.RemoveMRPeerResponse), verrors.ToStatusError(err)
}

func (s *server) GetStorageNodes(ctx context.Context, req *vmspb.GetStorageNodesRequest) (*vmspb.GetStorageNodesResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/GetStorageNodes", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			snmds, err := s.admin.storageNodes(ctx)
			return &vmspb.GetStorageNodesResponse{StorageNodes: snmds}, err
		},
	)
	return rspI.(*vmspb.GetStorageNodesResponse), verrors.ToStatusError(err)
}

func (s *server) Trim(ctx context.Context, req *vmspb.TrimRequest) (*vmspb.TrimResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManagerDeprecated/Trim", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			res, err := s.admin.trim(ctx, req.TopicID, req.LastGLSN)
			return &vmspb.TrimResponse{Results: res}, err
		},
	)
	return rspI.(*vmspb.TrimResponse), verrors.ToStatusError(err)
}
