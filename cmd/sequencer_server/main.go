package main

import (
	"log"
	"net"
	"runtime"

	sequencer "github.daumkakao.com/wokl/wokl/cmd/sequencer_server/sequencer"
	pb "github.daumkakao.com/wokl/wokl/proto/sequencer"
	"google.golang.org/grpc"
)

func main() {
	log.Printf("GOMAXPROCS: %v\n", runtime.GOMAXPROCS(0))
	lis, err := net.Listen("tcp", ":9091")
	if err != nil {
		log.Fatalf("could not listen: %v", err)
	}
	service, err := sequencer.NewSequencerService()
	if err != nil {
		log.Fatalf("could create sequencer service: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterSequencerServiceServer(s, service)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("could not serve: %v", err)
	}
}
