package sequencer

import (
	"context"

	pb "github.daumkakao.com/solar/solar/proto/sequencer"
	"google.golang.org/grpc"
)

type SequencerService struct {
	pb.UnimplementedSequencerServiceServer
	sqr Sequencer
}

func NewSequencerService(sqr Sequencer) *SequencerService {
	return &SequencerService{
		sqr: sqr,
	}
}

func (s *SequencerService) Register(server *grpc.Server) {
	pb.RegisterSequencerServiceServer(server, s)
}

func (s *SequencerService) Next(ctx context.Context, req *pb.SequencerRequest) (*pb.SequencerResponse, error) {
	return &pb.SequencerResponse{Glsn: s.sqr.Next()}, nil
}
