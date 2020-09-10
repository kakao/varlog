package storage

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	snpb "github.com/kakao/varlog/proto/storage_node"

	"google.golang.org/grpc"
)

type LogIOService struct {
	snpb.UnimplementedLogIOServer
	storageNodeID types.StorageNodeID
	lseGetter     LogStreamExecutorGetter
}

func NewLogIOService(storageNodeID types.StorageNodeID, lseGetter LogStreamExecutorGetter) *LogIOService {
	return &LogIOService{
		storageNodeID: storageNodeID,
		lseGetter:     lseGetter,
	}
}

func (s *LogIOService) Register(server *grpc.Server) {
	snpb.RegisterLogIOServer(server, s)
}

func (s *LogIOService) Append(ctx context.Context, req *snpb.AppendRequest) (*snpb.AppendResponse, error) {
	lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
	if !ok {
		return nil, varlog.ErrInvalidArgument
	}
	// TODO: create child context by using operation timeout
	// TODO: create replicas by using request
	glsn, err := lse.Append(ctx, req.GetPayload())
	if err != nil {
		return nil, varlog.ToStatusError(err)
	}
	return &snpb.AppendResponse{GLSN: glsn}, nil
}

func (s *LogIOService) Read(ctx context.Context, req *snpb.ReadRequest) (*snpb.ReadResponse, error) {
	lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
	if !ok {
		return nil, varlog.ErrInvalid
	}

	// TODO: create child context by using operation timeout
	data, err := lse.Read(ctx, req.GetGLSN())
	if err != nil {
		return nil, varlog.ToStatusError(err)
	}
	return &snpb.ReadResponse{Payload: data, GLSN: req.GetGLSN()}, nil
}

func (s *LogIOService) Subscribe(req *snpb.SubscribeRequest, stream snpb.LogIO_SubscribeServer) error {
	// FIXME: wrap error code by using grpc.status package
	//
	lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
	if !ok {
		return varlog.ErrInvalid
	}
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	c, err := lse.Subscribe(ctx, req.GetGLSN())
	if err != nil {
		return varlog.ToStatusError(err)
	}
	for r := range c {
		if r.err != nil {
			return r.err
		}
		err := stream.Send(&snpb.SubscribeResponse{
			GLSN:    r.logEntry.GLSN,
			LLSN:    r.logEntry.LLSN,
			Payload: r.logEntry.Data,
		})
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (s *LogIOService) Trim(ctx context.Context, req *snpb.TrimRequest) (*pbtypes.Empty, error) {
	var err error
	for _, lse := range s.lseGetter.GetLogStreamExecutors() {
		if e := lse.Trim(ctx, req.GetGLSN()); e != nil {
			err = e
		}
	}
	return &pbtypes.Empty{}, varlog.ToStatusError(err)
}
