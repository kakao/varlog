package metadata_repository

import (
	"context"

	pb "github.daumkakao.com/solar/solar/proto/metadata_repository"
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
	/*
		clientEpoch := req.GetEpoch()
		proposedProjection := solar.Projection{
			Epoch: req.GetProjection().GetEpoch(),
		}
		s.metaRepos.Propose(req.GetEpoch(), req.GetProjection())
	*/
	return nil, nil
}

func (s *MetadataRepositoryService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	projection, err := s.metaRepos.Get()
	if err != nil {
		return nil, err
	}
	rsp := &pb.GetResponse{
		Projection: projection,
	}
	return rsp, nil
}
