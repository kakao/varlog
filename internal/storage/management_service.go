package storage

import (
	"context"

	"github.com/gogo/protobuf/types"
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type managementService struct {
	pb.UnimplementedManagementServer

	m Management
}

func (s *managementService) GetMetadata(context.Context, *pb.GetMetadataRequest) (*pb.GetMetadataResponse, error) {
	panic("not yet implemented")
}

func (s *managementService) AddLogStream(ctx context.Context, req *pb.AddLogStreamRequest) (*pb.AddLogStreamResponse, error) {
	path, err := s.m.AddLogStream(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID(), req.GetStorage().GetPath())
	if err != nil {
		return nil, err
	}
	return &pb.AddLogStreamResponse{
		LogStream: &vpb.LogStreamDescriptor{
			LogStreamID: req.GetLogStreamID(),
			Status:      vpb.LogStreamStatusNormal,
			Replicas: []*vpb.ReplicaDescriptor{{
				StorageNodeID: req.GetStorageNodeID(),
				Path:          path,
			}},
		},
	}, nil
}

func (s *managementService) RemoveLogStream(ctx context.Context, req *pb.RemoveLogStreamRequest) (*types.Empty, error) {
	err := s.m.RemoveLogStream(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID())
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

func (s *managementService) Seal(context.Context, *pb.SealRequest) (*pb.SealResponse, error) {
	panic("not yet implemented")
}

func (s *managementService) Unseal(context.Context, *pb.UnsealRequest) (*types.Empty, error) {
	panic("not yet implemented")
}

func (s *managementService) Sync(context.Context, *pb.SyncRequest) (*pb.SyncResponse, error) {
	panic("not yet implemented")
}
