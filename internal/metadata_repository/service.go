package metadata_repository

import (
	"context"

	"github.com/gogo/protobuf/types"
	pb "github.com/kakao/varlog/proto/metadata_repository"
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
	if err != nil {
		return &types.Empty{}, err
	}

	return &types.Empty{}, nil
}

func (s *MetadataRepositoryService) RegisterLogStream(ctx context.Context, req *pb.LogStreamRequest) (*types.Empty, error) {
	err := s.metaRepos.RegisterLogStream(ctx, req.LogStream)
	if err != nil {
		return &types.Empty{}, err
	}
	return &types.Empty{}, nil
}

func (s *MetadataRepositoryService) UpdateLogStream(ctx context.Context, req *pb.LogStreamRequest) (*types.Empty, error) {
	err := s.metaRepos.UpdateLogStream(ctx, req.LogStream)
	if err != nil {
		return &types.Empty{}, err
	}
	return &types.Empty{}, nil
}

func (s *MetadataRepositoryService) GetMetadata(ctx context.Context, req *pb.GetMetadataRequest) (*pb.GetMetadataResponse, error) {
	metadata, err := s.metaRepos.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}
	rsp := &pb.GetMetadataResponse{
		Metadata: metadata,
	}
	return rsp, nil
}
