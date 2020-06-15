package storage

import (
	"context"

	pb "github.com/kakao/varlog/proto/storage_node"
	"google.golang.org/grpc"
)

type StorageNodeService struct {
	pb.UnimplementedStorageNodeServiceServer
	storage Storage
}

func NewStorageNodeService(storage Storage) *StorageNodeService {
	return &StorageNodeService{
		storage: storage,
	}

}

func (s *StorageNodeService) Register(server *grpc.Server) {
	pb.RegisterStorageNodeServiceServer(server, s)
}

func (s *StorageNodeService) Append(context.Context, *pb.AppendRequest) (*pb.AppendResponse, error) {
	panic("not yet implemented")
}

func (s *StorageNodeService) Read(context.Context, *pb.ReadRequest) (*pb.ReadResponse, error) {
	panic("not yet implemented")
}

func (s *StorageNodeService) Subscribe(*pb.SubscribeRequest, pb.StorageNodeService_SubscribeServer) error {
	panic("not yet implemented")
}

func (s *StorageNodeService) Trim(context.Context, *pb.TrimRequest) (*pb.TrimResponse, error) {
	panic("not yet implemented")
}
