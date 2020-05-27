package storage

import (
	"context"

	"github.daumkakao.com/solar/solar/pkg/solar"
	pb "github.daumkakao.com/solar/solar/proto/storage_node"
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

func (s *StorageNodeService) Call(ctx context.Context, req *pb.StorageNodeRequest) (*pb.StorageNodeResponse, error) {
	var err error
	rsp := &pb.StorageNodeResponse{
		Api: req.GetApi(),
	}
	epoch := req.GetEpoch()
	glsn := req.GetGlsn()
	data := req.GetData()
	switch req.GetApi() {
	case pb.READ:
		data, err = s.storage.Read(epoch, glsn)
		rsp.Data = data
	case pb.APPEND:
		err = s.storage.Append(epoch, glsn, data)
	case pb.FILL:
		err = s.storage.Fill(epoch, glsn)
	case pb.TRIM:
		err = s.storage.Trim(epoch, glsn)
	case pb.SEAL:
		err = s.storage.Seal(epoch, &rsp.MaxLsn)
	}
	// s.setReturnCode(err, rsp)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (s *StorageNodeService) read(epoch uint64, glsn uint64, rsp *pb.StorageNodeResponse) error {
	data, err := s.storage.Read(epoch, glsn)
	rsp.Data = data
	return err
}

func (s *StorageNodeService) setReturnCode(err error, rsp *pb.StorageNodeResponse) {
	switch err {
	case nil:
		rsp.ReturnCode = pb.OK
	case solar.ErrWrittenLogEntry:
		rsp.ReturnCode = pb.WRITTEN
	case solar.ErrTrimmedLogEntry:
		rsp.ReturnCode = pb.TRIMMED
	case solar.ErrUnwrittenLogEntry:
		rsp.ReturnCode = pb.UNWRITTEN
	case solar.ErrSealedEpoch:
		rsp.ReturnCode = pb.SEALED
	}
}
