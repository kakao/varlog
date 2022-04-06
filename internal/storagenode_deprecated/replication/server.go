package replication

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/stopchannel"
	"github.com/kakao/varlog/internal/storagenode_deprecated/rpcserver"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

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
	panic("not implemented")
}

func (s *serverImpl) ReplicateDeprecated(stream snpb.Replicator_ReplicateDeprecatedServer) error {
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
			err := rt.err
			rt.release()
			if ok && err == nil {
				continue
			}
			if !ok {
				return errors.WithStack(verrors.ErrClosed)
			}
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (s *serverImpl) recv(ctx context.Context, stream snpb.Replicator_ReplicateDeprecatedServer) <-chan *replicateTask {
	s.pipelines.Add(1)
	c := make(chan *replicateTask, s.pipelineQueueSize)
	go func() {
		defer s.pipelines.Done()
		defer close(c)
		var req snpb.ReplicationRequest
		for {
			err := stream.RecvMsg(&req)
			if err == nil {
				s.metrics.ReplicateRequestPropagationTime.Record(
					ctx,
					float64(time.Now().UnixMicro()-req.CreatedTime)/1000.0,
				)
			}
			repCtx := newReplicateTask()
			repCtx.req = req
			repCtx.err = err
			select {
			case c <- repCtx:
				if err != nil {
					return
				}
				s.metrics.ReplicateServerRequestQueueTasks.Add(ctx, 1)
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
				s.metrics.ReplicateServerRequestQueueTime.Record(ctx, float64(time.Since(repCtx.createdTime).Microseconds())/1000.0)
				s.metrics.ReplicateServerRequestQueueTasks.Add(ctx, -1)
				startTime := time.Now()
				tpid := repCtx.req.GetTopicID()
				lsid := repCtx.req.GetLogStreamID()
				if logReplicator, ok := s.logReplicatorGetter.Replicator(tpid, lsid); ok {
					err = logReplicator.Replicate(ctx, repCtx.req.GetLLSN(), repCtx.req.GetPayload())
				} else {
					err = fmt.Errorf("no executor: %v", lsid)
				}
				repCtx.replicatedTime = time.Now()
				repCtx.err = err
				s.metrics.RPCServerReplicateDuration.Record(
					ctx,
					float64(time.Since(startTime).Microseconds())/1000.0,
				)
			}
			select {
			case c <- repCtx:
				if err != nil {
					return
				}
				s.metrics.ReplicateServerResponseQueueTasks.Add(ctx, 1)
			case <-ctx.Done():
				return
			}
		}
	}()
	return c
}

func (s *serverImpl) send(ctx context.Context, stream snpb.Replicator_ReplicateDeprecatedServer, repCtxC <-chan *replicateTask) <-chan *replicateTask {
	s.pipelines.Add(1)
	c := make(chan *replicateTask, s.pipelineQueueSize)
	go func() {
		defer s.pipelines.Done()
		defer close(c)

		var err error
		rsp := &snpb.ReplicationResponse{}
		for repCtx := range repCtxC {
			err = repCtx.err
			if repCtx.err == nil {
				s.metrics.ReplicateServerResponseQueueTime.Record(ctx, float64(time.Since(repCtx.replicatedTime).Microseconds())/1000.0)
				s.metrics.ReplicateServerResponseQueueTasks.Add(ctx, -1)

				rsp.LLSN = repCtx.req.GetLLSN()
				rsp.CreatedTime = time.Now().UnixMicro()
				err = stream.SendMsg(rsp)
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
	}()

	tpID := req.GetDestination().TopicID
	lsID := req.GetDestination().LogStreamID
	logReplicator, ok := s.logReplicatorGetter.Replicator(tpID, lsID)
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
	}()

	tpID := req.GetDestination().TopicID
	lsID := req.GetDestination().LogStreamID
	logReplicator, ok := s.logReplicatorGetter.Replicator(tpID, lsID)
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
