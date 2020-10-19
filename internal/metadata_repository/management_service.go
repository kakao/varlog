package metadata_repository

import (
	"context"

	"github.com/gogo/protobuf/types"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"google.golang.org/grpc"
)

type ManagementService struct {
	mrpb.UnimplementedManagementServer
	m Management
}

func NewManagementService(m Management) *ManagementService {
	return &ManagementService{
		m: m,
	}
}

func (s *ManagementService) Register(server *grpc.Server) {
	mrpb.RegisterManagementServer(server, s)
}

func (s *ManagementService) AddPeer(ctx context.Context, req *mrpb.AddPeerRequest) (*types.Empty, error) {
	err := s.m.AddPeer(ctx, req.ClusterID, req.NodeID, req.Url)
	return &types.Empty{}, err
}

func (s *ManagementService) RemovePeer(ctx context.Context, req *mrpb.RemovePeerRequest) (*types.Empty, error) {
	err := s.m.RemovePeer(ctx, req.ClusterID, req.NodeID)
	return &types.Empty{}, err
}

func (s *ManagementService) GetClusterInfo(ctx context.Context, req *mrpb.GetClusterInfoRequest) (*mrpb.GetClusterInfoResponse, error) {
	cinfo, err := s.m.GetClusterInfo(ctx, req.ClusterID)
	return &mrpb.GetClusterInfoResponse{
		ClusterInfo: cinfo,
	}, err
}
