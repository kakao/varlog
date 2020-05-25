package main

import (
	"log"
	"net"
	"runtime"

	app "github.daumkakao.com/wokl/solar/cmd/storage_node/app"
	pb "github.daumkakao.com/wokl/solar/proto/storage_node"
	"google.golang.org/grpc"
)

func main() {
	log.Printf("GOMAXPROCS: %v\n", runtime.GOMAXPROCS(0))
	lis, err := net.Listen("tcp", ":9091")
	if err != nil {
		log.Fatalf("could not listen: %v", err)
	}
	service, err := app.NewStorageNodeService()
	if err != nil {
		log.Fatalf("could create storage node service: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterStorageNodeServiceServer(s, service)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("could not serve: %v", err)
	}
}
