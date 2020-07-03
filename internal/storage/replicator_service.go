package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/storage_node"
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
	logStreamID   types.LogStreamID
	lse           LogStreamExecutor
	pb.UnimplementedReplicatorServiceServer
}

func NewReplicatorService(storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, lse LogStreamExecutor) *ReplicatorService {
	return &ReplicatorService{
		storageNodeID: storageNodeID,
		logStreamID:   logStreamID,
		lse:           lse,
	}
}

func (s *ReplicatorService) Register(server *grpc.Server) {
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
				err = s.lse.Replicate(ctx, repCtx.req.GetLLSN(), repCtx.req.GetPayload())
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
					LogStreamID:   s.logStreamID,
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
