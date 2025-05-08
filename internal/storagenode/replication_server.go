package storagenode

import (
	"context"
	"fmt"
	"io"
	"sync"

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
	syncRange, err := lse.SyncInit(ctx, req.Source, req.Range, req.LastCommittedLLSN)
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

func (rs *replicationServer) SyncReplicateStream(stream snpb.Replicator_SyncReplicateStreamServer) error {
	var err error
	req := new(snpb.SyncReplicateRequest)
	for {
		req.Reset()
		err = stream.RecvMsg(req)
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("replication server: sync replicate stream: %w", err)
			}
			break
		}

		lse, loaded := rs.sn.executors.Load(req.Destination.TopicID, req.Destination.LogStreamID)
		if !loaded {
			err = fmt.Errorf("replication server: sync replicate stream: no log stream %v", req.Destination.LogStreamID)
			break
		}

		err = lse.SyncReplicate(stream.Context(), req.Source, req.Payload)
		if err != nil {
			err = fmt.Errorf("replication server: sync replicate stream: %w", err)
			break
		}
	}
	return multierr.Append(err, stream.SendAndClose(&snpb.SyncReplicateResponse{}))
}

func (rs *replicationServer) recv(ctx context.Context, stream snpb.Replicator_ReplicateServer, wg *sync.WaitGroup) <-chan *logstream.ReplicationTask {
	wg.Add(1)
	// TODO: add configuration
	c := make(chan *logstream.ReplicationTask, 4096)
	go func() {
		defer wg.Done()
		defer close(c)
		for {
			rst := logstream.NewReplicationTask()
			err := stream.RecvMsg(&rst.Req)
			rst.Err = err
			select {
			case c <- rst:
				if err != nil {
					return
				}
			case <-ctx.Done():
				rst.Release()
				return
			}
		}
	}()
	return c
}

func (rs *replicationServer) replicate(ctx context.Context, requestC <-chan *logstream.ReplicationTask, wg *sync.WaitGroup) <-chan error {
	wg.Add(1)
	errC := make(chan error)
	go func() {
		var err error
		defer func() {
			errC <- err
			close(errC)
			wg.Done()
		}()
		var rt *logstream.ReplicationTask
		var lse *logstream.Executor
		var ok bool
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			case rt, ok = <-requestC:
			}
			if !ok {
				return
			}
			err = rt.Err
			if err != nil {
				rt.Release()
				return
			}

			if lse == nil {
				var loaded bool
				lse, loaded = rs.sn.executors.Load(rt.Req.TopicID, rt.Req.LogStreamID)
				if !loaded {
					err = fmt.Errorf("replication server: no log stream %v", rt.Req.LogStreamID)
					rt.Release()
					return
				}
			}

			lse.Metrics().ReplicateServerOperations.Add(1)

			err = lse.Replicate(ctx, rt)
			if err != nil {
				rt.Release()
				return
			}
		}
	}()
	return errC
}
