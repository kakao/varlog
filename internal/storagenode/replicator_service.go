package storagenode

import (
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/label"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
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
	tmStub        *telemetryStub
	logger        *zap.Logger
}

var _ snpb.ReplicatorServer = (*ReplicatorService)(nil)

func NewReplicatorService(storageNodeID types.StorageNodeID, lseGetter LogStreamExecutorGetter, tmStub *telemetryStub, logger *zap.Logger) *ReplicatorService {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("replicatorservice")
	return &ReplicatorService{
		storageNodeID: storageNodeID,
		lseGetter:     lseGetter,
		tmStub:        tmStub,
		logger:        logger,
	}
}

func (s *ReplicatorService) Register(server *grpc.Server) {
	s.logger.Info("register to rpc server")
	snpb.RegisterReplicatorServer(server, s)
}

func (s *ReplicatorService) Replicate(stream snpb.Replicator_ReplicateServer) (err error) {
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	ctx := stream.Context()
	c := s.send(ctx, stream, s.replicate(ctx, s.recv(ctx, stream)))
	for repCtx := range c {
		if repCtx.err == io.EOF {
			err = nil
			return nil
		}
		if repCtx.err != nil {
			err = repCtx.err
			return repCtx.err
		}
	}
	// TODO: use proper error and message
	err = fmt.Errorf("stream is broken")
	return err
}

func (s *ReplicatorService) recv(ctx context.Context, stream snpb.Replicator_ReplicateServer) <-chan *replicationContext {
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

func (s *ReplicatorService) send(ctx context.Context, stream snpb.Replicator_ReplicateServer, repCtxC <-chan *replicationContext) <-chan *replicationContext {
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

func (s *ReplicatorService) SyncReplicate(ctx context.Context, req *snpb.SyncReplicateRequest) (rsp *snpb.SyncReplicateResponse, err error) {
	var spanName = "varlog.snpb.Replicator/SyncReplicate"
	ctx, span := s.tmStub.startSpan(ctx, spanName,
		oteltrace.WithAttributes(label.StorageNodeIDLabel(s.storageNodeID)),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
	)
	s.tmStub.metrics().activeRequests.Add(ctx, 1, label.String("call", spanName))
	defer func() {
		if err == nil {
			s.logger.Info("SyncReplicate",
				zap.String("request", req.String()),
				zap.String("response", rsp.String()),
			)
		} else {
			s.logger.Error("SyncReplicate",
				zap.Error(err),
				zap.String("request", req.String()),
			)
		}
		s.tmStub.metrics().activeRequests.Add(ctx, -1, label.String("call", spanName))
		span.End()
	}()

	lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
	if !ok {
		err = errors.Errorf("no logstreamexecutor: %v", req.GetLogStreamID())
		return rsp, err
	}
	err = lse.SyncReplicate(ctx, req.GetFirst(), req.GetLast(), req.GetCurrent(), req.GetData())
	rsp = &snpb.SyncReplicateResponse{}
	return rsp, err
}
