package storage

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type managementService struct {
	snpb.UnimplementedManagementServer

	logger *zap.Logger
	m      Management
}

func NewManagementService(m Management, logger *zap.Logger) *managementService {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("managementservice")
	return &managementService{logger: logger, m: m}
}

func (s *managementService) Register(server *grpc.Server) {
	s.logger.Info("register to rpc server")
	snpb.RegisterManagementServer(server, s)
}

// GetMetadata implements the ManagementServer GetMetadata method.
func (s *managementService) GetMetadata(ctx context.Context, req *snpb.GetMetadataRequest) (*snpb.GetMetadataResponse, error) {
	metadata, err := s.m.GetMetadata(req.GetClusterID(), req.GetMetadataType())
	if err != nil {
		s.logger.Error("could not get metadata", zap.Error(err))
		return nil, err
	}
	return &snpb.GetMetadataResponse{StorageNodeMetadata: metadata}, nil
}

// AddLogStream implements the ManagementServer AddLogStream method.
func (s *managementService) AddLogStream(ctx context.Context, req *snpb.AddLogStreamRequest) (*snpb.AddLogStreamResponse, error) {
	path, err := s.m.AddLogStream(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID(), req.GetStorage().GetPath())
	if err != nil {
		s.logger.Error("could not add logstream", zap.Error(err))
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
	err := s.m.RemoveLogStream(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID())
	if err != nil {
		s.logger.Error("could not remove logstream", zap.Error(err))
		return nil, err
	}
	return &pbtypes.Empty{}, nil
}

// Seal implements the ManagementServer Seal method.
func (s *managementService) Seal(ctx context.Context, req *snpb.SealRequest) (*snpb.SealResponse, error) {
	status, maxGLSN, err := s.m.Seal(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID(), req.GetLastCommittedGLSN())
	if err != nil {
		s.logger.Error("could not seal", zap.Error(err))
		return nil, err
	}
	return &snpb.SealResponse{
		Status:            status,
		LastCommittedGLSN: maxGLSN,
	}, nil
}

// Unseal implements the ManagementServer Unseal method.
func (s *managementService) Unseal(ctx context.Context, req *snpb.UnsealRequest) (*pbtypes.Empty, error) {
	err := s.m.Unseal(req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID())
	if err != nil {
		s.logger.Error("could not unseal", zap.Error(err))
		return nil, err
	}
	return &pbtypes.Empty{}, nil
}

// Sync implements the ManagementServer Sync method.
func (s *managementService) Sync(context.Context, *snpb.SyncRequest) (*snpb.SyncResponse, error) {
	panic("not yet implemented")
}
