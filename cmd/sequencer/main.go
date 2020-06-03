package main

import (
	"log"
	"net"
	"runtime"

	sequencer "github.daumkakao.com/varlog/varlog/internal/sequencer"
	pb "github.daumkakao.com/varlog/varlog/proto/sequencer"
	"google.golang.org/grpc"
)

func main() {
	log.Printf("GOMAXPROCS: %v\n", runtime.GOMAXPROCS(0))
	lis, err := net.Listen("tcp", ":9091")
	if err != nil {
		log.Fatalf("could not listen: %v", err)
	}
	sqr := sequencer.NewSequencer()
	service := sequencer.NewSequencerService(sqr)
	if err != nil {
		log.Fatalf("could create sequencer service: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterSequencerServiceServer(s, service)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("could not serve: %v", err)
	}
}
