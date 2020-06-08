package metadata_repository

import (
	"context"

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

func (s *MetadataRepositoryService) Propose(ctx context.Context, req *pb.ProposeRequest) (*pb.ProposeResponse, error) {
	s.metaRepos.Propose(req.GetEpoch(), req.GetProjection())
	return &pb.ProposeResponse{}, nil
}

func (s *MetadataRepositoryService) GetProjection(ctx context.Context, req *pb.GetProjectionRequest) (*pb.GetProjectionResponse, error) {
	projection, err := s.metaRepos.GetProjection(req.Epoch)
	if err != nil {
		return nil, err
	}
	rsp := &pb.GetProjectionResponse{
		Projection: projection,
	}
	return rsp, nil
}

func (s *MetadataRepositoryService) GetMetadata(ctx context.Context, req *pb.GetMetadataRequest) (*pb.GetMetadataResponse, error) {
	metadata, err := s.metaRepos.GetMetadata()
	if err != nil {
		return nil, err
	}
	rsp := &pb.GetMetadataResponse{
		Metadata: metadata,
	}
	return rsp, nil
}
