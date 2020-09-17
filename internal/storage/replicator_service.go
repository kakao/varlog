package storage

import (
	"context"
	"fmt"
	"io"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type replicationContext struct {
	req *pb.ReplicationRequest
	err error
}

// TODO: configurable...
const (
	replicationContextCSize = 0
)

type ReplicatorService struct {
	storageNodeID types.StorageNodeID
	lseGetter     LogStreamExecutorGetter
	pb.UnimplementedReplicatorServiceServer
	logger *zap.Logger
}

func NewReplicatorService(storageNodeID types.StorageNodeID, lseGetter LogStreamExecutorGetter, logger *zap.Logger) *ReplicatorService {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("replicatorservice")
	return &ReplicatorService{
		storageNodeID: storageNodeID,
		lseGetter:     lseGetter,
		logger:        logger,
	}
}

func (s *ReplicatorService) Register(server *grpc.Server) {
	s.logger.Info("register to rpc server")
	pb.RegisterReplicatorServiceServer(server, s)
}

func (s *ReplicatorService) Replicate(stream pb.ReplicatorService_ReplicateServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.send(ctx, stream, s.replicate(ctx, s.recv(ctx, stream)))
	for repCtx := range c {
		if repCtx.err == io.EOF {
			return nil
		}
		if repCtx.err != nil {
			return repCtx.err
		}
	}
	// TODO: use proper error and message
	return fmt.Errorf("stream is broken")
}

func (s *ReplicatorService) recv(ctx context.Context, stream pb.ReplicatorService_ReplicateServer) <-chan *replicationContext {
	c := make(chan *replicationContext, replicationContextCSize)
	go func() {
		defer close(c)
		var req *pb.ReplicationRequest
		var err error
		for {
			req, err = stream.Recv()
			repCtx := &replicationContext{
				req: req,
				err: err,
			}
			select {
			case c <- repCtx:
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return c
}

func (s *ReplicatorService) replicate(ctx context.Context, repCtxC <-chan *replicationContext) <-chan *replicationContext {
	c := make(chan *replicationContext, replicationContextCSize)
	go func() {
		defer close(c)
		var err error
		for repCtx := range repCtxC {
			if repCtx.err == nil {
				lsid := repCtx.req.GetLogStreamID()
				if lse, ok := s.lseGetter.GetLogStreamExecutor(lsid); ok {
					err = lse.Replicate(ctx, repCtx.req.GetLLSN(), repCtx.req.GetPayload())
				} else {
					err = fmt.Errorf("no logstreamexecutor: %v", lsid)
				}
				repCtx.err = err
			}
			select {
			case c <- repCtx:
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return c
}

func (s *ReplicatorService) send(ctx context.Context, stream pb.ReplicatorService_ReplicateServer, repCtxC <-chan *replicationContext) <-chan *replicationContext {
	c := make(chan *replicationContext, replicationContextCSize)
	go func() {
		defer close(c)
		var err error
		for repCtx := range repCtxC {
			if repCtx.err == nil {
				err = stream.Send(&pb.ReplicationResponse{
					StorageNodeID: s.storageNodeID,
					LogStreamID:   repCtx.req.GetLogStreamID(),
					LLSN:          repCtx.req.GetLLSN(),
				})
				repCtx.err = err
			}
			select {
			case c <- repCtx:
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return c
}
