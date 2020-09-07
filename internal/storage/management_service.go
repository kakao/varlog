package storage

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	snpb "github.com/kakao/varlog/proto/storage_node"
	vpb "github.com/kakao/varlog/proto/varlog"
	"google.golang.org/grpc"
)

type managementService struct {
	snpb.UnimplementedManagementServer

	m Management
}

func NewManagementService(m Management) *managementService {
	return &managementService{m: m}
}

func (s *managementService) Register(server *grpc.Server) {
	snpb.RegisterManagementServer(server, s)
}

// GetMetadata implements the ManagementServer GetMetadata method.
func (s *managementService) GetMetadata(ctx context.Context, req *snpb.GetMetadataRequest) (*snpb.GetMetadataResponse, error) {
	metadata, err := s.m.GetMetadata(req.GetClusterID(), req.GetMetadataType())
	if err != nil {
		return nil, err
	}
	return &snpb.GetMetadataResponse{StorageNodeMetadata: metadata}, nil
}

// AddLogStream implements the ManagementServer AddLogStream method.
func (s *managementService) AddLogStream(ctx context.Context, req *snpb.AddLogStreamRequest) (*snpb.AddLogStreamResponse, error) {
	if !verifyIDs(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID()) {
		return nil, varlog.ErrInvalidArgument
	}
	path, err := s.m.AddLogStream(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID(), req.GetStorage().GetPath())
	if err != nil {
		return nil, err
	}
	return &snpb.AddLogStreamResponse{
		LogStream: &vpb.LogStreamDescriptor{
			LogStreamID: req.GetLogStreamID(),
			Status:      vpb.LogStreamStatusRunning,
			Replicas: []*vpb.ReplicaDescriptor{{
				StorageNodeID: req.GetStorageNodeID(),
				Path:          path,
			}},
		},
	}, nil
}

// RemoveLogStream implements the ManagementServer RemoveLogStream method.
func (s *managementService) RemoveLogStream(ctx context.Context, req *snpb.RemoveLogStreamRequest) (*pbtypes.Empty, error) {
	if !verifyIDs(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID()) {
		return nil, varlog.ErrInvalidArgument
	}
	err := s.m.RemoveLogStream(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID())
	if err != nil {
		return nil, err
	}
	return &pbtypes.Empty{}, nil
}

// Seal implements the ManagementServer Seal method.
func (s *managementService) Seal(ctx context.Context, req *snpb.SealRequest) (*snpb.SealResponse, error) {
	if !verifyIDs(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID()) {
		return nil, varlog.ErrInvalidArgument
	}
	status, maxGLSN, err := s.m.Seal(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID(), req.GetLastCommittedGLSN())
	if err != nil {
		return nil, err
	}
	return &snpb.SealResponse{
		Status:            status,
		LastCommittedGLSN: maxGLSN,
	}, nil
}

// Unseal implements the ManagementServer Unseal method.
func (s *managementService) Unseal(ctx context.Context, req *snpb.UnsealRequest) (*pbtypes.Empty, error) {
	if !verifyIDs(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID()) {
		return nil, varlog.ErrInvalidArgument
	}
	err := s.m.Unseal(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID())
	if err != nil {
		return nil, err
	}
	return &pbtypes.Empty{}, nil
}

// Sync implements the ManagementServer Sync method.
func (s *managementService) Sync(context.Context, *snpb.SyncRequest) (*snpb.SyncResponse, error) {
	panic("not yet implemented")
}

func verifyIDs(cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID) bool {
	// TODO: check the range of each IDs
	return true
}
