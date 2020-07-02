package metadata_repository

import (
	"context"

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

func (s *MetadataRepositoryService) RegisterStorageNode(ctx context.Context, req *pb.RegisterStorageNodeRequest) (*pb.RegisterStorageNodeResponse, error) {
	err := s.metaRepos.RegisterStorageNode(ctx, req.StorageNode)
	if err != nil {
		return nil, err
	}

	rsp := &pb.RegisterStorageNodeResponse{}
	return rsp, nil
}

func (s *MetadataRepositoryService) CreateLogStream(ctx context.Context, req *pb.CreateLogStreamRequest) (*pb.CreateLogStreamResponse, error) {
	err := s.metaRepos.CreateLogStream(ctx, req.LogStream)
	if err != nil {
		return nil, err
	}
	rsp := &pb.CreateLogStreamResponse{}
	return rsp, nil
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
