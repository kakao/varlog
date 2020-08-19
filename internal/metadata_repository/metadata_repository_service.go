package metadata_repository

import (
	"context"

	"github.com/gogo/protobuf/types"
	pb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
	"google.golang.org/grpc"
)

type MetadataRepositoryService struct {
	pb.UnimplementedMetadataRepositoryServiceServer
	metaRepos MetadataRepository
}

func NewMetadataRepositoryService(metaRepos MetadataRepository) *MetadataRepositoryService {
	return &MetadataRepositoryService{
		metaRepos: metaRepos,
	}
}

func (s *MetadataRepositoryService) Register(server *grpc.Server) {
	pb.RegisterMetadataRepositoryServiceServer(server, s)
}

func (s *MetadataRepositoryService) RegisterStorageNode(ctx context.Context, req *pb.StorageNodeRequest) (*types.Empty, error) {
	err := s.metaRepos.RegisterStorageNode(ctx, req.StorageNode)
	return &types.Empty{}, err
}

func (s *MetadataRepositoryService) UnregisterStorageNode(ctx context.Context, req *pb.StorageNodeRequest) (*types.Empty, error) {
	err := s.metaRepos.UnregisterStorageNode(ctx, req.StorageNode.StorageNodeID)
	return &types.Empty{}, err
}

func (s *MetadataRepositoryService) RegisterLogStream(ctx context.Context, req *pb.LogStreamRequest) (*types.Empty, error) {
	err := s.metaRepos.RegisterLogStream(ctx, req.LogStream)
	return &types.Empty{}, err
}

func (s *MetadataRepositoryService) UnregisterLogStream(ctx context.Context, req *pb.LogStreamRequest) (*types.Empty, error) {
	err := s.metaRepos.UnregisterLogStream(ctx, req.LogStream.LogStreamID)
	return &types.Empty{}, err
}

func (s *MetadataRepositoryService) UpdateLogStream(ctx context.Context, req *pb.LogStreamRequest) (*types.Empty, error) {
	err := s.metaRepos.UpdateLogStream(ctx, req.LogStream)
	return &types.Empty{}, err
}

func (s *MetadataRepositoryService) GetMetadata(ctx context.Context, req *pb.GetMetadataRequest) (*pb.GetMetadataResponse, error) {
	metadata, err := s.metaRepos.GetMetadata(ctx)
	if err != nil {
		return &pb.GetMetadataResponse{}, err
	}

	return &pb.GetMetadataResponse{
		Metadata: metadata,
	}, nil
}
