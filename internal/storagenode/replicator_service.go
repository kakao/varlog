package storagenode

import (
	"context"
	"fmt"
	"io"

	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/proto/snpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type replicationContext struct {
	req *snpb.ReplicationRequest
	err error
}

// TODO: configurable...
const (
	replicationContextCSize = 0
)

type ReplicatorService struct {
	storageNodeID types.StorageNodeID
	lseGetter     LogStreamExecutorGetter
	snpb.UnimplementedReplicatorServiceServer
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
	snpb.RegisterReplicatorServiceServer(server, s)
}

func (s *ReplicatorService) Replicate(stream snpb.ReplicatorService_ReplicateServer) error {
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

func (s *ReplicatorService) recv(ctx context.Context, stream snpb.ReplicatorService_ReplicateServer) <-chan *replicationContext {
	c := make(chan *replicationContext, replicationContextCSize)
	go func() {
		defer close(c)
		var req *snpb.ReplicationRequest
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

func (s *ReplicatorService) send(ctx context.Context, stream snpb.ReplicatorService_ReplicateServer, repCtxC <-chan *replicationContext) <-chan *replicationContext {
	c := make(chan *replicationContext, replicationContextCSize)
	go func() {
		defer close(c)
		var err error
		for repCtx := range repCtxC {
			if repCtx.err == nil {
				err = stream.Send(&snpb.ReplicationResponse{
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

func (s *ReplicatorService) SyncReplicate(ctx context.Context, req *snpb.SyncReplicateRequest) (*snpb.SyncReplicateResponse, error) {
	lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
	if !ok {
		return nil, fmt.Errorf("no logstreamexecutor: %v", req.GetLogStreamID())
	}
	err := lse.SyncReplicate(ctx, req.GetFirst(), req.GetLast(), req.GetCurrent(), req.GetData())
	return &snpb.SyncReplicateResponse{}, err
}
