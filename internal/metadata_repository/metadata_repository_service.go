package metadata_repository

import (
	"context"

	"github.com/gogo/protobuf/types"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"google.golang.org/grpc"
)

type MetadataRepositoryService struct {
	metaRepos MetadataRepository
}

var _ mrpb.MetadataRepositoryServiceServer = (*MetadataRepositoryService)(nil)

func NewMetadataRepositoryService(metaRepos MetadataRepository) *MetadataRepositoryService {
	return &MetadataRepositoryService{
		metaRepos: metaRepos,
	}
}

func (s *MetadataRepositoryService) Register(server *grpc.Server) {
	mrpb.RegisterMetadataRepositoryServiceServer(server, s)
}

func (s *MetadataRepositoryService) RegisterStorageNode(ctx context.Context, req *mrpb.StorageNodeRequest) (*types.Empty, error) {
	err := s.metaRepos.RegisterStorageNode(ctx, req.StorageNode)
	return &types.Empty{}, err
}

func (s *MetadataRepositoryService) UnregisterStorageNode(ctx context.Context, req *mrpb.StorageNodeRequest) (*types.Empty, error) {
	err := s.metaRepos.UnregisterStorageNode(ctx, req.StorageNode.StorageNodeID)
	return &types.Empty{}, err
}

func (s *MetadataRepositoryService) RegisterLogStream(ctx context.Context, req *mrpb.LogStreamRequest) (*types.Empty, error) {
	err := s.metaRepos.RegisterLogStream(ctx, req.LogStream)
	return &types.Empty{}, err
}

func (s *MetadataRepositoryService) UnregisterLogStream(ctx context.Context, req *mrpb.LogStreamRequest) (*types.Empty, error) {
	err := s.metaRepos.UnregisterLogStream(ctx, req.LogStream.LogStreamID)
	return &types.Empty{}, err
}

func (s *MetadataRepositoryService) UpdateLogStream(ctx context.Context, req *mrpb.LogStreamRequest) (*types.Empty, error) {
	err := s.metaRepos.UpdateLogStream(ctx, req.LogStream)
	return &types.Empty{}, err
}

func (s *MetadataRepositoryService) GetMetadata(ctx context.Context, req *mrpb.GetMetadataRequest) (*mrpb.GetMetadataResponse, error) {
	metadata, err := s.metaRepos.GetMetadata(ctx)
	return &mrpb.GetMetadataResponse{
		Metadata: metadata,
	}, err
}

func (s *MetadataRepositoryService) Seal(ctx context.Context, req *mrpb.SealRequest) (*mrpb.SealResponse, error) {
	lastCommittedGLSN, err := s.metaRepos.Seal(ctx, req.GetLogStreamID())
	return &mrpb.SealResponse{LastCommittedGLSN: lastCommittedGLSN}, err
}

func (s *MetadataRepositoryService) Unseal(ctx context.Context, req *mrpb.UnsealRequest) (*mrpb.UnsealResponse, error) {
	err := s.metaRepos.Unseal(ctx, req.GetLogStreamID())
	return &mrpb.UnsealResponse{}, err
}
