package vms

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/vmspb"
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
	s.logger.Info("register to rpc server")
	vmspb.RegisterClusterManagerServer(server, s)
}

func (s *clusterManagerService) AddStorageNode(ctx context.Context, req *vmspb.AddStorageNodeRequest) (*vmspb.AddStorageNodeResponse, error) {
	snmeta, err := s.clusManager.AddStorageNode(ctx, req.GetAddress())
	s.logger.Info("AddStorageNode", zap.String("snmeta", snmeta.String()), zap.Error(err))
	return &vmspb.AddStorageNodeResponse{StorageNode: snmeta}, verrors.ToStatusError(err)
}

func (s *clusterManagerService) UnregisterStorageNode(ctx context.Context, req *vmspb.UnregisterStorageNodeRequest) (*vmspb.UnregisterStorageNodeResponse, error) {
	err := s.clusManager.UnregisterStorageNode(ctx, req.GetStorageNodeID())
	return &vmspb.UnregisterStorageNodeResponse{}, verrors.ToStatusError(err)
}

func (s *clusterManagerService) AddLogStream(ctx context.Context, req *vmspb.AddLogStreamRequest) (*vmspb.AddLogStreamResponse, error) {
	logStreamDesc, err := s.clusManager.AddLogStream(ctx, req.GetReplicas())
	s.logger.Info("AddLogStream", zap.String("lsdesc", logStreamDesc.String()))
	return &vmspb.AddLogStreamResponse{LogStream: logStreamDesc}, verrors.ToStatusError(err)
}

func (s *clusterManagerService) UnregisterLogStream(ctx context.Context, req *vmspb.UnregisterLogStreamRequest) (*vmspb.UnregisterLogStreamResponse, error) {
	err := s.clusManager.UnregisterLogStream(ctx, req.GetLogStreamID())
	return &vmspb.UnregisterLogStreamResponse{}, verrors.ToStatusError(err)
}

func (s *clusterManagerService) RemoveLogStreamReplica(ctx context.Context, req *vmspb.RemoveLogStreamReplicaRequest) (*vmspb.RemoveLogStreamReplicaResponse, error) {
	err := s.clusManager.RemoveLogStreamReplica(ctx, req.GetStorageNodeID(), req.GetLogStreamID())
	return &vmspb.RemoveLogStreamReplicaResponse{}, verrors.ToStatusError(err)
}

func (s *clusterManagerService) UpdateLogStream(ctx context.Context, req *vmspb.UpdateLogStreamRequest) (*vmspb.UpdateLogStreamResponse, error) {
	lsdesc, err := s.clusManager.UpdateLogStream(ctx, req.GetLogStreamID(), req.GetPoppedReplica(), req.GetPushedReplica())
	return &vmspb.UpdateLogStreamResponse{LogStream: lsdesc}, verrors.ToStatusError(err)
}

func (s *clusterManagerService) Seal(ctx context.Context, req *vmspb.SealRequest) (*vmspb.SealResponse, error) {
	lsmetas, err := s.clusManager.Seal(ctx, req.GetLogStreamID())
	return &vmspb.SealResponse{LogStreams: lsmetas}, verrors.ToStatusError(err)
}

func (s *clusterManagerService) Sync(ctx context.Context, req *vmspb.SyncRequest) (*vmspb.SyncResponse, error) {
	status, err := s.clusManager.Sync(ctx, req.GetLogStreamID(), req.GetSrcStorageNodeID(), req.GetDstStorageNodeID())
	return &vmspb.SyncResponse{Status: status}, verrors.ToStatusError(err)
}

func (s *clusterManagerService) Unseal(ctx context.Context, req *vmspb.UnsealRequest) (*vmspb.UnsealResponse, error) {
	lsdesc, err := s.clusManager.Unseal(ctx, req.GetLogStreamID())
	return &vmspb.UnsealResponse{LogStream: lsdesc}, verrors.ToStatusError(err)
}

func (s *clusterManagerService) GetMRMembers(ctx context.Context, _ *pbtypes.Empty) (*vmspb.GetMRMembersResponse, error) {
	mrInfo, err := s.clusManager.MRInfos(ctx)
	if err != nil {
		return &vmspb.GetMRMembersResponse{}, err
	}

	resp := &vmspb.GetMRMembersResponse{
		Leader:            mrInfo.Leader,
		ReplicationFactor: mrInfo.ReplicationFactor,
	}

	resp.Members = make(map[types.NodeID]string)
	for nodeID, m := range mrInfo.Members {
		resp.Members[nodeID] = m.Peer
	}

	return resp, verrors.ToStatusError(err)
}
