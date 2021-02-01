package vms

import (
	"context"
	"fmt"

	pbtypes "github.com/gogo/protobuf/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
)

type clusterManagerService struct {
	clusManager ClusterManager
	logger      *zap.Logger
}

var _ vmspb.ClusterManagerServer = (*clusterManagerService)(nil)

func NewClusterManagerService(clusterManager ClusterManager, logger *zap.Logger) *clusterManagerService {
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
		s.logger.Info(spanName, zap.Stringer("request", req.(fmt.Stringer)), zap.Stringer("response", rsp.(fmt.Stringer)))
	} else {
		s.logger.Error(spanName, zap.Stringer("request", req.(fmt.Stringer)), zap.Error(err))
	}
	return rsp, err
}

func (s *clusterManagerService) AddStorageNode(ctx context.Context, req *vmspb.AddStorageNodeRequest) (*vmspb.AddStorageNodeResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/AddStorageNode", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*vmspb.AddStorageNodeRequest)
			snmeta, err := s.clusManager.AddStorageNode(ctx, req.GetAddress())
			return &vmspb.AddStorageNodeResponse{StorageNode: snmeta}, err
		},
	)
	return rspI.(*vmspb.AddStorageNodeResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) UnregisterStorageNode(ctx context.Context, req *vmspb.UnregisterStorageNodeRequest) (*vmspb.UnregisterStorageNodeResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/UnregisterStorageNode", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			err := s.clusManager.UnregisterStorageNode(ctx, req.GetStorageNodeID())
			return &vmspb.UnregisterStorageNodeResponse{}, err
		},
	)
	return rspI.(*vmspb.UnregisterStorageNodeResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) AddLogStream(ctx context.Context, req *vmspb.AddLogStreamRequest) (*vmspb.AddLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/AddLogStream", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			logStreamDesc, err := s.clusManager.AddLogStream(ctx, req.GetReplicas())
			return &vmspb.AddLogStreamResponse{LogStream: logStreamDesc}, err
		},
	)
	return rspI.(*vmspb.AddLogStreamResponse), verrors.ToStatusErrorWithCode(err, codes.Unavailable)
}

func (s *clusterManagerService) UnregisterLogStream(ctx context.Context, req *vmspb.UnregisterLogStreamRequest) (*vmspb.UnregisterLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/UnregisterLogStream", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			err := s.clusManager.UnregisterLogStream(ctx, req.GetLogStreamID())
			return &vmspb.UnregisterLogStreamResponse{}, err
		},
	)
	return rspI.(*vmspb.UnregisterLogStreamResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) RemoveLogStreamReplica(ctx context.Context, req *vmspb.RemoveLogStreamReplicaRequest) (*vmspb.RemoveLogStreamReplicaResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/RemoveLogStreamReplica", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			err := s.clusManager.RemoveLogStreamReplica(ctx, req.GetStorageNodeID(), req.GetLogStreamID())
			return &vmspb.RemoveLogStreamReplicaResponse{}, err
		},
	)
	return rspI.(*vmspb.RemoveLogStreamReplicaResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) UpdateLogStream(ctx context.Context, req *vmspb.UpdateLogStreamRequest) (*vmspb.UpdateLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/UpdateLogStream", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			lsdesc, err := s.clusManager.UpdateLogStream(ctx, req.GetLogStreamID(), req.GetPoppedReplica(), req.GetPushedReplica())
			return &vmspb.UpdateLogStreamResponse{LogStream: lsdesc}, err
		},
	)
	return rspI.(*vmspb.UpdateLogStreamResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) Seal(ctx context.Context, req *vmspb.SealRequest) (*vmspb.SealResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/Seal", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			lsmetas, err := s.clusManager.Seal(ctx, req.GetLogStreamID())
			return &vmspb.SealResponse{LogStreams: lsmetas}, err
		},
	)
	return rspI.(*vmspb.SealResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) Sync(ctx context.Context, req *vmspb.SyncRequest) (*vmspb.SyncResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/Sync", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			status, err := s.clusManager.Sync(ctx, req.GetLogStreamID(), req.GetSrcStorageNodeID(), req.GetDstStorageNodeID())
			return &vmspb.SyncResponse{Status: status}, err
		},
	)
	return rspI.(*vmspb.SyncResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) Unseal(ctx context.Context, req *vmspb.UnsealRequest) (*vmspb.UnsealResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/Unseal", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			lsdesc, err := s.clusManager.Unseal(ctx, req.GetLogStreamID())
			return &vmspb.UnsealResponse{LogStream: lsdesc}, err
		},
	)
	return rspI.(*vmspb.UnsealResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) GetMRMembers(ctx context.Context, req *pbtypes.Empty) (*vmspb.GetMRMembersResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/GetMRMembers", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
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
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			nodeID, err := s.clusManager.AddMRPeer(ctx, req.RaftURL, req.RPCAddr)
			return &vmspb.AddMRPeerResponse{NodeID: nodeID}, err
		},
	)
	return rspI.(*vmspb.AddMRPeerResponse), verrors.ToStatusError(err)
}

func (s *clusterManagerService) GetStorageNodes(ctx context.Context, req *pbtypes.Empty) (*vmspb.GetStorageNodesResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.vmspb.ClusterManager/GetStorageNodes", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			var rsp *vmspb.GetStorageNodesResponse
			meta, err := s.clusManager.Metadata(ctx)
			if err != nil {
				return rsp, err
			}
			rsp = &vmspb.GetStorageNodesResponse{}
			if meta != nil && meta.StorageNodes != nil {
				rsp.Storagenodes = make(map[types.StorageNodeID]string, len(meta.StorageNodes))
				for _, sn := range meta.StorageNodes {
					rsp.Storagenodes[sn.StorageNodeID] = sn.Address
				}
			}
			return rsp, nil
		},
	)
	return rspI.(*vmspb.GetStorageNodesResponse), verrors.ToStatusError(err)
}
