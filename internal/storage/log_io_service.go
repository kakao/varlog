package storage

import (
	"context"
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/storage_node"
	"google.golang.org/grpc"
)

type LogIOService struct {
	pb.UnimplementedLogIOServer
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
	pb.RegisterLogIOServer(server, s)
}

func (s *LogIOService) Append(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
	if !ok {
		return nil, varlog.ErrInvalid
	}
	// TODO: create child context by using operation timeout
	// TODO: create replicas by using request
	glsn, err := lse.Append(ctx, req.GetPayload())
	if err != nil {
		return nil, err
	}
	return &pb.AppendResponse{GLSN: glsn}, nil
}

func (s *LogIOService) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
	if !ok {
		return nil, varlog.ErrInvalid
	}

	// TODO: create child context by using operation timeout
	data, err := lse.Read(ctx, req.GetGLSN())
	if err != nil {
		return nil, err
	}
	return &pb.ReadResponse{Payload: data, GLSN: req.GetGLSN()}, nil
}

func (s *LogIOService) Subscribe(req *pb.SubscribeRequest, stream pb.LogIO_SubscribeServer) error {
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
		return err
	}
	for r := range c {
		if r.err != nil {
			return r.err
		}
		err := stream.Send(&pb.SubscribeResponse{
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

func (s *LogIOService) Trim(ctx context.Context, req *pb.TrimRequest) (*pb.TrimResponse, error) {
	targetLSEs := s.lseGetter.GetLogStreamExecutors()

	// NOTE: subtle case
	// If the trim operation will remove very large GLSN that is not stored yet, current LSEs
	// remove all log entries. After replied the trim operation, log entries within the scope
	// of removing will be saved again.

	// TODO: create child context by using operation timeout
	type result struct {
		num uint64
		err error
	}

	// NOTE: When a trimTask is enqueued, it can't be canceled by using the context passed by
	// the RPC handler. We have below options:
	// - Use the context (or its child context) to delete log entries
	// - All trim operations are asyncrhonous - use tombstone!
	// - Jus wait!
	c := make(chan result, len(targetLSEs))
	var wg sync.WaitGroup
	wg.Add(len(targetLSEs))
	for _, lse := range targetLSEs {
		go func(lse LogStreamExecutor) {
			defer wg.Done()
			cnt, err := lse.Trim(ctx, req.GetGLSN(), req.GetAsync())
			c <- result{cnt, err}
		}(lse)
	}
	wg.Wait()
	close(c)
	rsp := &pb.TrimResponse{}
	for res := range c {
		rsp.NumTrimmed += res.num
		if res.err != nil {
			return nil, res.err
		}
	}
	return rsp, nil
}
