package metadata_repository

import (
	"context"

	"github.com/gogo/protobuf/types"
	pb "github.com/kakao/varlog/proto/metadata_repository"
	"google.golang.org/grpc"
)

type ManagementService struct {
	pb.UnimplementedManagementServer
	m Management
}

func NewManagementService(m Management) *ManagementService {
	return &ManagementService{
		m: m,
	}
}

func (s *ManagementService) Register(server *grpc.Server) {
	pb.RegisterManagementServer(server, s)
}

func (s *ManagementService) AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*types.Empty, error) {
	err := s.m.AddPeer(ctx, req.ClusterID, req.NodeID, req.Url)
	return &types.Empty{}, err
}

func (s *ManagementService) RemovePeer(ctx context.Context, req *pb.RemovePeerRequest) (*types.Empty, error) {
	err := s.m.RemovePeer(ctx, req.ClusterID, req.NodeID)
	return &types.Empty{}, err
}

func (s *ManagementService) GetClusterInfo(ctx context.Context, req *pb.GetClusterInfoRequest) (*pb.GetClusterInfoResponse, error) {
	nodeID, urls, err := s.m.GetClusterInfo(ctx, req.ClusterID)
	return &pb.GetClusterInfoResponse{
		NodeID: nodeID,
		Urls:   urls,
	}, err
}
