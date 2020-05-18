package sequencer

import (
	"context"

	pb "github.daumkakao.com/wokl/wokl/proto/sequencer"
)

type SequencerService struct {
	pb.UnimplementedSequencerServiceServer
	sqr *sequencer
}

func NewSequencerService() (*SequencerService, error) {
	return &SequencerService{
		sqr: newSequencer(),
	}, nil
}

func (s *SequencerService) Next(ctx context.Context, req *pb.SequencerRequest) (*pb.SequencerResponse, error) {
	return &pb.SequencerResponse{Glsn: s.sqr.Next()}, nil
}
