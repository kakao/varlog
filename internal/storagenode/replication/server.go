package replication

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/pkg/errors"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/storagenode/rpcserver"
	"github.com/kakao/varlog/internal/storagenode/stopchannel"
	"github.com/kakao/varlog/pkg/util/telemetry/attribute"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

// TODO: use pool
type replicateTask struct {
	req *snpb.ReplicationRequest
	err error
}

type Server interface {
	io.Closer
	snpb.ReplicatorServer
	rpcserver.Registrable
}

type serverImpl struct {
	serverConfig
	barrier struct {
		running bool
		mu      sync.RWMutex
	}
	pipelines struct {
		sync.WaitGroup
		mu sync.Mutex
	}
	stopper *stopchannel.StopChannel
}

var _ Server = (*serverImpl)(nil)

func NewServer(opts ...ServerOption) *serverImpl {
	cfg := newServerConfig(opts)
	s := &serverImpl{
		serverConfig: cfg,
		stopper:      stopchannel.New(),
	}
	s.barrier.running = true
	return s
}

func (s *serverImpl) Register(server *grpc.Server) {
	s.logger.Info("register to rpc server")
	snpb.RegisterReplicatorServer(server, s)
}

func (s *serverImpl) Replicate(stream snpb.Replicator_ReplicateServer) error {
	s.barrier.mu.RLock()
	defer s.barrier.mu.RUnlock()
	if !s.barrier.running {
		return errors.WithStack(verrors.ErrClosed)
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	c := s.send(ctx, stream, s.replicate(ctx, s.recv(ctx, stream)))
	for {
		select {
		case <-s.stopper.StopC():
			return errors.WithStack(verrors.ErrClosed)
		case <-stream.Context().Done():
			return stream.Context().Err()
		case rt, ok := <-c:
			if ok && rt.err == nil {
				continue
			}
			if !ok {
				return errors.WithStack(verrors.ErrClosed)
			}
			if rt.err == io.EOF {
				return nil
			}
			return rt.err
		}
	}
}

func (s *serverImpl) recv(ctx context.Context, stream snpb.Replicator_ReplicateServer) <-chan *replicateTask {
	s.pipelines.Add(1)
	c := make(chan *replicateTask, s.pipelineQueueSize)
	go func() {
		defer s.pipelines.Done()
		defer close(c)
		var req *snpb.ReplicationRequest
		var err error
		for {
			req, err = stream.Recv()
			repCtx := &replicateTask{
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

func (s *serverImpl) replicate(ctx context.Context, repCtxC <-chan *replicateTask) <-chan *replicateTask {
	s.pipelines.Add(1)
	c := make(chan *replicateTask, s.pipelineQueueSize)
	go func() {
		defer s.pipelines.Done()
		defer close(c)
		var err error
		for repCtx := range repCtxC {
			err = repCtx.err
			if repCtx.err == nil {
				lsid := repCtx.req.GetLogStreamID()
				if logReplicator, ok := s.logReplicatorGetter.Replicator(lsid); ok {
					err = logReplicator.Replicate(ctx, repCtx.req.GetLLSN(), repCtx.req.GetPayload())
				} else {
					err = fmt.Errorf("no executor: %v", lsid)
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

func (s *serverImpl) send(ctx context.Context, stream snpb.Replicator_ReplicateServer, repCtxC <-chan *replicateTask) <-chan *replicateTask {
	s.pipelines.Add(1)
	c := make(chan *replicateTask, s.pipelineQueueSize)
	go func() {
		defer s.pipelines.Done()
		defer close(c)
		var err error
		for repCtx := range repCtxC {
			err = repCtx.err
			if repCtx.err == nil {
				err = stream.Send(&snpb.ReplicationResponse{
					StorageNodeID: s.storageNodeIDGetter.StorageNodeID(),
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

func (s *serverImpl) SyncInit(ctx context.Context, req *snpb.SyncInitRequest) (rsp *snpb.SyncInitResponse, err error) {
	s.barrier.mu.RLock()
	defer s.barrier.mu.RUnlock()
	if !s.barrier.running {
		return nil, errors.WithStack(verrors.ErrClosed)
	}

	var spanName = "varlog.snpb.Replicator/SyncInit"
	ctx, span := s.tmStub.StartSpan(ctx, spanName,
		oteltrace.WithAttributes(attribute.StorageNodeID(s.storageNodeIDGetter.StorageNodeID())),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
	)
	s.tmStub.Metrics().ActiveRequests.Add(ctx, 1, attribute.String("call", spanName))

	defer func() {
		if err == nil {
			s.logger.Info("SyncInit",
				zap.String("request", req.String()),
				zap.String("response", rsp.String()),
			)
		} else {
			s.logger.Error("SyncInit",
				zap.Error(err),
				zap.String("request", req.String()),
			)
		}
		s.tmStub.Metrics().ActiveRequests.Add(ctx, -1, attribute.String("call", spanName))
		span.End()
	}()

	lsID := req.GetDestination().LogStreamID
	logReplicator, ok := s.logReplicatorGetter.Replicator(lsID)
	if !ok {
		err = errors.Errorf("no executor: %v", lsID)
		return rsp, err
	}
	dstRange, err := logReplicator.SyncInit(ctx, req.GetRange())
	rsp = &snpb.SyncInitResponse{Range: dstRange}
	return rsp, err
}

func (s *serverImpl) SyncReplicate(ctx context.Context, req *snpb.SyncReplicateRequest) (rsp *snpb.SyncReplicateResponse, err error) {
	s.barrier.mu.RLock()
	defer s.barrier.mu.RUnlock()
	if !s.barrier.running {
		return nil, errors.WithStack(verrors.ErrClosed)
	}

	var spanName = "varlog.snpb.Replicator/SyncReplicate"
	ctx, span := s.tmStub.StartSpan(ctx, spanName,
		oteltrace.WithAttributes(attribute.StorageNodeID(s.storageNodeIDGetter.StorageNodeID())),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
	)
	s.tmStub.Metrics().ActiveRequests.Add(ctx, 1, attribute.String("call", spanName))
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
		s.tmStub.Metrics().ActiveRequests.Add(ctx, -1, attribute.String("call", spanName))
		span.End()
	}()

	lsID := req.GetDestination().LogStreamID
	logReplicator, ok := s.logReplicatorGetter.Replicator(lsID)
	if !ok {
		err = errors.Errorf("no executor: %v", lsID)
		return rsp, err
	}
	err = logReplicator.SyncReplicate(ctx, req.GetPayload())
	rsp = &snpb.SyncReplicateResponse{}
	return rsp, err
}

func (s *serverImpl) Close() error {
	s.stopper.Stop()
	s.barrier.mu.Lock()
	defer s.barrier.mu.Unlock()
	s.barrier.running = false
	s.pipelines.Wait()
	return nil
}
