package storagenode

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/proto/snpb"
)

type replicationServer struct {
	sn     *StorageNode
	logger *zap.Logger
}

var _ snpb.ReplicatorServer = (*replicationServer)(nil)

func (rs *replicationServer) Replicate(stream snpb.Replicator_ReplicateServer) error {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(stream.Context())
	defer func() {
		cancel()
		wg.Wait()
	}()
	errC := rs.replicate(ctx, rs.recv(ctx, stream, wg), wg)
	err := <-errC
	err = multierr.Append(err, stream.SendAndClose(&snpb.ReplicateResponse{}))
	rs.logger.Error("closed replication stream", zap.Error(err))
	return err
}

func (rs *replicationServer) SyncInit(ctx context.Context, req *snpb.SyncInitRequest) (*snpb.SyncInitResponse, error) {
	lse, loaded := rs.sn.executors.Load(req.Destination.TopicID, req.Destination.LogStreamID)
	if !loaded {
		return nil, fmt.Errorf("replication server: no log stream %v", req.Destination.LogStreamID)
	}
	syncRange, err := lse.SyncInit(ctx, req.Source, req.Range)
	return &snpb.SyncInitResponse{Range: syncRange}, err
}

func (rs *replicationServer) SyncReplicate(ctx context.Context, req *snpb.SyncReplicateRequest) (*snpb.SyncReplicateResponse, error) {
	lse, loaded := rs.sn.executors.Load(req.Destination.TopicID, req.Destination.LogStreamID)
	if !loaded {
		return nil, fmt.Errorf("replication server: no log stream %v", req.Destination.LogStreamID)
	}
	err := lse.SyncReplicate(ctx, req.Source, req.Payload)
	return &snpb.SyncReplicateResponse{}, err
}

var replicationServerTaskPool = sync.Pool{
	New: func() interface{} {
		return &replicationServerTask{}
	},
}

type replicationServerTask struct {
	req snpb.ReplicateRequest
	err error
}

func newReplicationServerTask(req snpb.ReplicateRequest, err error) *replicationServerTask {
	rst := replicationServerTaskPool.Get().(*replicationServerTask)
	rst.req = req
	rst.err = err
	return rst
}

func (rst *replicationServerTask) release() {
	rst.req = snpb.ReplicateRequest{}
	rst.err = nil
	replicationServerTaskPool.Put(rst)
}

func (rs *replicationServer) recv(ctx context.Context, stream snpb.Replicator_ReplicateServer, wg *sync.WaitGroup) <-chan *replicationServerTask {
	wg.Add(1)
	// TODO: add configuration
	c := make(chan *replicationServerTask, 4096)
	go func() {
		defer wg.Done()
		defer close(c)
		req := &snpb.ReplicateRequest{}
		for {
			err := stream.RecvMsg(req)
			rst := newReplicationServerTask(*req, err)
			select {
			case c <- rst:
				if err != nil {
					return
				}
			case <-ctx.Done():
				rst.release()
				return
			}
		}
	}()
	return c
}

func (rs *replicationServer) replicate(ctx context.Context, requestC <-chan *replicationServerTask, wg *sync.WaitGroup) <-chan error {
	wg.Add(1)
	errC := make(chan error)
	go func() {
		var err error
		defer func() {
			errC <- err
			close(errC)
			wg.Done()
		}()
		var rst *replicationServerTask
		var lse *logstream.Executor
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			case rst = <-requestC:
			}
			err = rst.err
			if err != nil {
				rst.release()
				return
			}

			if lse == nil {
				var loaded bool
				lse, loaded = rs.sn.executors.Load(rst.req.TopicID, rst.req.LogStreamID)
				if !loaded {
					err = fmt.Errorf("replication server: no log stream %v", rst.req.LogStreamID)
					rst.release()
					return
				}
			}

			atomic.AddInt64(&lse.Metrics().ReplicateServerOperations, 1)

			err = lse.Replicate(ctx, rst.req.LLSN, rst.req.Data)
			if err != nil {
				rst.release()
				return
			}
			rst.release()
		}
	}()
	return errC
}
