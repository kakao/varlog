package varlogadm

import (
	"context"
	"fmt"

	pbtypes "github.com/gogo/protobuf/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/vmspb"
)

type clusterManagerService struct {
	clusManager ClusterManager
	logger      *zap.Logger
}

var _ vmspb.ClusterManagerServer = (*clusterManagerService)(nil)

func newClusterManagerService(clusterManager ClusterManager, logger *zap.Logger) *clusterManagerService {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("vmsservice")
	return &clusterManagerService{
		clusManager: clusterManager,
		logger:      logger,
	}
}

func (s *clusterManagerService) Register(server *grpc.Server) {
	vmspb.RegisterClusterManagerServer(server, s)
	s.logger.Info("register to rpc server")
}

type handler func(ctx context.Context, req interface{}) (rsp interface{}, err error)

func (s *clusterManagerService) withTelemetry(ctx context.Context, spanName string, req interface{}, fn handler) (rsp interface{}, err error) {
	rsp, err = fn(ctx, req)
	if err == nil {
		s.logger.Debug(spanName, zap.Stringer("request", req.(fmt.Stringer)), zap.Stringer("response", rsp.(fmt.Stringer)))
	} else {
		s.logger.Error(spanName, zap.Stringer("request", req.(fmt.Stringer)), zap.Error(err))
	}
	return rsp, err
}

func (s *clusterManagerService) AddStorageNode(ctx context.Context, req *vmspb.AddStorageNodeRequest) (*vmspb.AddStorageNodeResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/AddStorageNode", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			snmeta, err := s.clusManager.AddStorageNode(ctx, req.GetAddress())
			return &vmspb.AddStorageNodeResponse{StorageNode: snmeta}, err
		},
	)
	return rspI.(*vmspb.AddStorageNodeResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) UnregisterStorageNode(ctx context.Context, req *vmspb.UnregisterStorageNodeRequest) (*vmspb.UnregisterStorageNodeResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/UnregisterStorageNode", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.clusManager.UnregisterStorageNode(ctx, req.GetStorageNodeID())
			return &vmspb.UnregisterStorageNodeResponse{}, err
		},
	)
	return rspI.(*vmspb.UnregisterStorageNodeResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) AddTopic(ctx context.Context, req *vmspb.AddTopicRequest) (*vmspb.AddTopicResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/AddTopic", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			topicDesc, err := s.clusManager.AddTopic(ctx)
			return &vmspb.AddTopicResponse{Topic: topicDesc}, err
		},
	)
	return rspI.(*vmspb.AddTopicResponse), verrors.ToStatusErrorWithCode(err, codes.Unavailable)
}

func (s *clusterManagerService) Topics(ctx context.Context, req *vmspb.TopicsRequest) (*vmspb.TopicsResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/Topics", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			tds, err := s.clusManager.Topics(ctx)
			return &vmspb.TopicsResponse{Topics: tds}, err
		},
	)
	return rspI.(*vmspb.TopicsResponse), verrors.ToStatusErrorWithCode(err, codes.Unavailable)
}

func (s *clusterManagerService) DescribeTopic(ctx context.Context, req *vmspb.DescribeTopicRequest) (*vmspb.DescribeTopicResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/DescribeTopic", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			td, lsds, err := s.clusManager.DescribeTopic(ctx, req.TopicID)
			return &vmspb.DescribeTopicResponse{
				Topic:      td,
				LogStreams: lsds,
			}, err
		},
	)
	return rspI.(*vmspb.DescribeTopicResponse), verrors.ToStatusErrorWithCode(err, codes.Unavailable)
}

func (s *clusterManagerService) UnregisterTopic(ctx context.Context, req *vmspb.UnregisterTopicRequest) (*vmspb.UnregisterTopicResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/UnregisterTopic", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.clusManager.UnregisterTopic(ctx, req.GetTopicID())
			return &vmspb.UnregisterTopicResponse{}, err
		},
	)
	return rspI.(*vmspb.UnregisterTopicResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) AddLogStream(ctx context.Context, req *vmspb.AddLogStreamRequest) (*vmspb.AddLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/AddLogStream", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			logStreamDesc, err := s.clusManager.AddLogStream(ctx, req.GetTopicID(), req.GetReplicas())
			return &vmspb.AddLogStreamResponse{LogStream: logStreamDesc}, err
		},
	)
	return rspI.(*vmspb.AddLogStreamResponse), verrors.ToStatusErrorWithCode(err, codes.Unavailable)
}

func (s *clusterManagerService) UnregisterLogStream(ctx context.Context, req *vmspb.UnregisterLogStreamRequest) (*vmspb.UnregisterLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/UnregisterLogStream", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.clusManager.UnregisterLogStream(ctx, req.GetTopicID(), req.GetLogStreamID())
			return &vmspb.UnregisterLogStreamResponse{}, err
		},
	)
	return rspI.(*vmspb.UnregisterLogStreamResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) RemoveLogStreamReplica(ctx context.Context, req *vmspb.RemoveLogStreamReplicaRequest) (*vmspb.RemoveLogStreamReplicaResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/RemoveLogStreamReplica", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.clusManager.RemoveLogStreamReplica(ctx, req.GetStorageNodeID(), req.GetTopicID(), req.GetLogStreamID())
			return &vmspb.RemoveLogStreamReplicaResponse{}, err
		},
	)
	return rspI.(*vmspb.RemoveLogStreamReplicaResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) UpdateLogStream(ctx context.Context, req *vmspb.UpdateLogStreamRequest) (*vmspb.UpdateLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/UpdateLogStream", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			lsdesc, err := s.clusManager.UpdateLogStream(ctx, req.GetLogStreamID(), req.GetPoppedReplica(), req.GetPushedReplica())
			return &vmspb.UpdateLogStreamResponse{LogStream: lsdesc}, err
		},
	)
	return rspI.(*vmspb.UpdateLogStreamResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) Seal(ctx context.Context, req *vmspb.SealRequest) (*vmspb.SealResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/Seal", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			lsmetas, sealedGLSN, err := s.clusManager.Seal(ctx, req.GetTopicID(), req.GetLogStreamID())
			return &vmspb.SealResponse{
				LogStreams: lsmetas,
				SealedGLSN: sealedGLSN,
			}, err
		},
	)
	return rspI.(*vmspb.SealResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) Sync(ctx context.Context, req *vmspb.SyncRequest) (*vmspb.SyncResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/Sync", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			status, err := s.clusManager.Sync(ctx, req.GetTopicID(), req.GetLogStreamID(), req.GetSrcStorageNodeID(), req.GetDstStorageNodeID())
			return &vmspb.SyncResponse{Status: status}, err
		},
	)
	return rspI.(*vmspb.SyncResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) Unseal(ctx context.Context, req *vmspb.UnsealRequest) (*vmspb.UnsealResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/Unseal", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			lsdesc, err := s.clusManager.Unseal(ctx, req.GetTopicID(), req.GetLogStreamID())
			return &vmspb.UnsealResponse{LogStream: lsdesc}, err
		},
	)
	return rspI.(*vmspb.UnsealResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) GetMRMembers(ctx context.Context, req *pbtypes.Empty) (*vmspb.GetMRMembersResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/GetMRMembers", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			var rsp *vmspb.GetMRMembersResponse
			mrInfo, err := s.clusManager.MRInfos(ctx)
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

func (s *clusterManagerService) AddMRPeer(ctx context.Context, req *vmspb.AddMRPeerRequest) (*vmspb.AddMRPeerResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/AddMRPeer", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			nodeID, err := s.clusManager.AddMRPeer(ctx, req.RaftURL, req.RPCAddr)
			return &vmspb.AddMRPeerResponse{NodeID: nodeID}, err
		},
	)
	return rspI.(*vmspb.AddMRPeerResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) RemoveMRPeer(ctx context.Context, req *vmspb.RemoveMRPeerRequest) (*vmspb.RemoveMRPeerResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/RemoveMRPeer", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.clusManager.RemoveMRPeer(ctx, req.RaftURL)
			return &vmspb.RemoveMRPeerResponse{}, err
		},
	)
	return rspI.(*vmspb.RemoveMRPeerResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) GetStorageNodes(ctx context.Context, req *vmspb.GetStorageNodesRequest) (*vmspb.GetStorageNodesResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/GetStorageNodes", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			snmds, err := s.clusManager.StorageNodes(ctx)
			return &vmspb.GetStorageNodesResponse{StorageNodes: snmds}, err
		},
	)
	return rspI.(*vmspb.GetStorageNodesResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) Trim(ctx context.Context, req *vmspb.TrimRequest) (*vmspb.TrimResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/Trim", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			res, err := s.clusManager.Trim(ctx, req.TopicID, req.LastGLSN)
			return &vmspb.TrimResponse{Results: res}, err
		},
	)
	return rspI.(*vmspb.TrimResponse), verrors.ToStatusError(err)
}
