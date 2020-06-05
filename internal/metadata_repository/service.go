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

func (s *MetadataRepositoryService) Propose(ctx context.Context, req *pb.ProposeRequest) (*pb.ProposeResponse, error) {
	s.metaRepos.Propose(req.GetEpoch(), req.GetProjection())
	return &pb.ProposeResponse{}, nil
}

func (s *MetadataRepositoryService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	projection, err := s.metaRepos.Get(req.Epoch)
	if err != nil {
		return nil, err
	}
	rsp := &pb.GetResponse{
		Projection: projection,
	}
	return rsp, nil
}
