package main

import (
	"log"
	"runtime"
)

func main() {
	log.Printf("GOMAXPROCS: %v\n", runtime.GOMAXPROCS(0))
	/*
		lis, err := net.Listen("tcp", ":9091")
		if err != nil {
			log.Fatalf("could not listen: %v", err)
		}
		metaRepos := metadata_repository.NewInMemoryMetadataRepository()
		service := metadata_repository.NewMetadataRepositoryService(metaRepos)
		s := grpc.NewServer()
		pb.RegisterMetadataRepositoryServiceServer(s, service)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("could not serve: %v", err)
		}
	*/
}
