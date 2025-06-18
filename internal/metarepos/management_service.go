package metarepos

import (
	"context"

	gogotypes "github.com/gogo/protobuf/types"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/mrpb"
)

type ManagementService struct {
	m Management
}

var _ mrpb.ManagementServer = (*ManagementService)(nil)

func NewManagementService(m Management) *ManagementService {
	return &ManagementService{
		m: m,
	}
}

func (s *ManagementService) Register(server *grpc.Server) {
	mrpb.RegisterManagementServer(server, s)
}

func (s *ManagementService) AddPeer(ctx context.Context, req *mrpb.AddPeerRequest) (*gogotypes.Empty, error) {
	// For backward compatibility, we allow the request to contain either Peer or ClusterID/NodeID/Url.
	clusterID := req.Peer.ClusterID
	nodeID := req.Peer.NodeID
	url := req.Peer.URL

	// Prior to the introduction of Peer (v0.23.0), AddPeerRequest doesn't contain Peer.
	//
	// TODO: Remove this backward compatibility code in the future.
	if clusterID.Invalid() || nodeID == types.InvalidNodeID || url == "" {
		clusterID = req.ClusterID
		nodeID = req.NodeID
		url = req.Url
	}

	err := s.m.AddPeer(ctx, clusterID, nodeID, url)
	return &gogotypes.Empty{}, err
}

func (s *ManagementService) RemovePeer(ctx context.Context, req *mrpb.RemovePeerRequest) (*gogotypes.Empty, error) {
	err := s.m.RemovePeer(ctx, req.ClusterID, req.NodeID)
	return &gogotypes.Empty{}, err
}

func (s *ManagementService) GetClusterInfo(ctx context.Context, req *mrpb.GetClusterInfoRequest) (*mrpb.GetClusterInfoResponse, error) {
	cinfo, err := s.m.GetClusterInfo(ctx, req.ClusterID)
	return &mrpb.GetClusterInfoResponse{
		ClusterInfo: cinfo,
	}, err
}
