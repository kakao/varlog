package main

import (
	"log"
	"net"
	"runtime"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/storage_node"
	"google.golang.org/grpc"
)

type sstorage struct{}

func (s *sstorage) Read(glsn types.GLSN) ([]byte, error) {
	panic("not yet implemented")
}

func (s *sstorage) Write(llsn types.LLSN, data []byte) error {
	panic("not yet implemented")
}

func (s *sstorage) Commit(llsn types.LLSN, glsn types.GLSN) error {
	panic("not yet implemented")
}

func (s *sstorage) Delete(glsn types.GLSN) error {
	panic("not yet implemented")
}

func main() {
	log.Printf("GOMAXPROCS: %v\n", runtime.GOMAXPROCS(0))
	lis, err := net.Listen("tcp", ":9091")
	if err != nil {
		log.Fatalf("could not listen: %v", err)
	}
	stg := &sstorage{}
	service := storage.NewStorageNodeService(stg)
	if err != nil {
		log.Fatalf("could create storage node service: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterStorageNodeServiceServer(s, service)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("could not serve: %v", err)
	}
}
