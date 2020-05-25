package app

import (
	"context"

	"github.daumkakao.com/wokl/solar/internal/sequencer"
	pb "github.daumkakao.com/wokl/solar/proto/sequencer"
)

type SequencerService struct {
	pb.UnimplementedSequencerServiceServer
	sqr *sequencer.Sequencer
}

func NewSequencerService() (*SequencerService, error) {
	return &SequencerService{
		sqr: sequencer.NewSequencer(),
	}, nil
}

func (s *SequencerService) Next(ctx context.Context, req *pb.SequencerRequest) (*pb.SequencerResponse, error) {
	return &pb.SequencerResponse{Glsn: s.sqr.Next()}, nil
}
