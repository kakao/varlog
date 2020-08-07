package storage

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/storage_node"
	vpb "github.com/kakao/varlog/proto/varlog"
)

type managementService struct {
	pb.UnimplementedManagementServer

	m Management
}

func (s *managementService) GetMetadata(ctx context.Context, req *pb.GetMetadataRequest) (*pb.GetMetadataResponse, error) {
	metadata, err := s.m.GetMetadata(req.GetClusterID(), req.GetMetadataType())
	if err != nil {
		return nil, err
	}
	return &pb.GetMetadataResponse{StorageNodeMetadata: metadata}, nil
}

func (s *managementService) AddLogStream(ctx context.Context, req *pb.AddLogStreamRequest) (*pb.AddLogStreamResponse, error) {
	if !verifyIDs(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID()) {
		return nil, varlog.ErrInvalidArgument
	}
	path, err := s.m.AddLogStream(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID(), req.GetStorage().GetPath())
	if err != nil {
		return nil, err
	}
	return &pb.AddLogStreamResponse{
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

func (s *managementService) RemoveLogStream(ctx context.Context, req *pb.RemoveLogStreamRequest) (*pbtypes.Empty, error) {
	if !verifyIDs(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID()) {
		return nil, varlog.ErrInvalidArgument
	}
	err := s.m.RemoveLogStream(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID())
	if err != nil {
		return nil, err
	}
	return &pbtypes.Empty{}, nil
}

func (s *managementService) Seal(ctx context.Context, req *pb.SealRequest) (*pb.SealResponse, error) {
	if !verifyIDs(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID()) {
		return nil, varlog.ErrInvalidArgument
	}
	status, maxGLSN, err := s.m.Seal(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID(), req.GetLastCommittedGLSN())
	if err != nil {
		return nil, err
	}
	return &pb.SealResponse{
		Status:            status,
		LastCommittedGLSN: maxGLSN,
	}, nil
}

func (s *managementService) Unseal(ctx context.Context, req *pb.UnsealRequest) (*pbtypes.Empty, error) {
	if !verifyIDs(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID()) {
		return nil, varlog.ErrInvalidArgument
	}
	err := s.m.Unseal(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID())
	if err != nil {
		return nil, err
	}
	return &pbtypes.Empty{}, nil
}

func (s *managementService) Sync(context.Context, *pb.SyncRequest) (*pb.SyncResponse, error) {
	panic("not yet implemented")
}

func verifyIDs(cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID) bool {
	// TODO: check the range of each IDs
	return true
}
