package replication

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode/replication -package replication -destination client_mock.go . Client

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/kakao/varlog/internal/storagenode/jobqueue"
	"github.com/kakao/varlog/internal/storagenode/stopchannel"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type Client interface {
	io.Closer
	Replicate(ctx context.Context, llsn types.LLSN, data []byte, cb func(error))
	PeerStorageNodeID() types.StorageNodeID
	SyncInit(ctx context.Context, srcRnage snpb.SyncRange) (snpb.SyncRange, error)
	SyncReplicate(ctx context.Context, replica varlogpb.Replica, payload snpb.SyncPayload) error
}

type client struct {
	clientConfig

	connector *connector

	rpcConn   *rpc.Conn
	rpcClient snpb.ReplicatorClient

	requestQ  jobqueue.JobQueue
	callbackQ jobqueue.JobQueue

	dispatchers sync.WaitGroup
	stopper     *stopchannel.StopChannel
	closed      struct {
		val bool
		mu  sync.RWMutex
	}
}

var _ Client = (*client)(nil)

// Add more detailed peer info (e.g., storage node id)
func newClient(ctx context.Context, opts ...ClientOption) (*client, error) {
	cfg, err := newClientConfig(opts)
	if err != nil {
		return nil, err
	}

	c := &client{
		clientConfig: *cfg,
		stopper:      stopchannel.New(),
	}

	c.callbackQ, err = jobqueue.NewChQueue(c.requestQueueSize)
	if err != nil {
		return nil, err
	}

	c.requestQ, err = jobqueue.NewChQueue(c.requestQueueSize)
	if err != nil {
		return nil, err
	}

	if err = c.run(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *client) run(ctx context.Context) (err error) {
	c.rpcConn, err = rpc.NewConn(ctx, c.replica.GetAddress())
	if err != nil {
		return err
	}
	c.rpcClient = snpb.NewReplicatorClient(c.rpcConn.Conn)
	stream, err := c.rpcClient.Replicate(context.Background())
	if err != nil {
		return multierr.Append(errors.WithStack(err), c.rpcConn.Close())
	}
	c.dispatchers.Add(2)
	go c.sendLoop(stream)
	go c.recvLoop(stream)
	return nil
}

func (c *client) PeerStorageNodeID() types.StorageNodeID {
	return c.replica.StorageNode.StorageNodeID
}

func (c *client) Replicate(ctx context.Context, llsn types.LLSN, data []byte, callback func(error)) {
	startTime := time.Now()

	var err error

	c.closed.mu.RLock()
	defer func() {
		c.closed.mu.RUnlock()
		if err != nil {
			callback(err)
		}
	}()

	if c.closed.val {
		err = errors.WithStack(verrors.ErrClosed)
		return
	}

	cb := newCallbackBlock(llsn, callback)
	err = c.callbackQ.PushWithContext(ctx, cb)
	if err != nil {
		cb.release()
		return
	}

	req := &snpb.ReplicationRequest{
		TopicID:     c.replica.GetTopicID(),
		LogStreamID: c.replica.GetLogStreamID(),
		LLSN:        llsn,
		Payload:     data,
		CreatedTime: time.Now(),
	}
	err = c.requestQ.PushWithContext(ctx, req)
	if err != nil {
		return
	}

	c.measure.Stub().Metrics().ExecutorReplicateRequestPrepareTime.Record(
		ctx,
		float64(time.Since(startTime).Microseconds())/1000.0,
	)
}

func (c *client) sendLoop(stream snpb.Replicator_ReplicateClient) {
	defer c.dispatchers.Done()

Loop:
	for {
		select {
		case <-stream.Context().Done():
			break Loop
		case <-c.stopper.StopC():
			break Loop
		default:
		}

		reqI, err := c.requestQ.PopWithContext(stream.Context())
		if err != nil {
			break Loop
		}
		req := reqI.(*snpb.ReplicationRequest)
		now := time.Now()
		c.measure.Stub().Metrics().ExecutorReplicateClientRequestQueueTime.Record(
			context.Background(),
			float64(now.Sub(req.CreatedTime).Microseconds())/1000.0,
		)
		req.CreatedTime = now
		if err := stream.Send(req); err != nil {
			break Loop
		}
	}

	_ = stream.CloseSend()
}

func (c *client) recvLoop(stream snpb.Replicator_ReplicateClient) {
	defer c.dispatchers.Done()

	var (
		err error
		rsp *snpb.ReplicationResponse
	)
Loop:
	for {
		select {
		case <-stream.Context().Done():
			err = stream.Context().Err()
			break Loop
		default:
		}

		rsp, err = stream.Recv()
		if err != nil {
			break Loop
		}
		/*
			c.measure.Stub().Metrics().ExecutorReplicateResponsePropagationTime.Record(
				context.Background(),
				float64(time.Since(rsp.GetCreatedTime()).Microseconds())/1000.0,
			)
		*/

		// TODO: Check if this is safe.
		cb := c.callbackQ.Pop().(*callbackBlock)
		if cb.llsn != rsp.GetLLSN() {
			panic(errors.Errorf("llsn mismatch: %d != %d", cb.llsn, rsp.GetLLSN()))
		}
		cb.callback(nil)
		cb.release()
	}

	for c.callbackQ.Size() > 0 {
		cb := c.callbackQ.Pop().(*callbackBlock)
		cb.callback(err)
		cb.release()
	}
}

func (c *client) SyncInit(ctx context.Context, srcRnage snpb.SyncRange) (snpb.SyncRange, error) {
	c.closed.mu.RLock()
	if c.closed.val {
		c.closed.mu.RUnlock()
		return snpb.InvalidSyncRange(), errors.WithStack(verrors.ErrClosed)
	}
	defer c.closed.mu.RUnlock()

	rsp, err := c.rpcClient.SyncInit(ctx, &snpb.SyncInitRequest{
		Destination: c.replica,
		Range:       srcRnage,
	})
	return rsp.GetRange(), errors.WithStack(verrors.FromStatusError(err))
}

func (c *client) SyncReplicate(ctx context.Context, replica varlogpb.Replica, payload snpb.SyncPayload) error {
	c.closed.mu.RLock()
	if c.closed.val {
		c.closed.mu.RUnlock()
		return errors.WithStack(verrors.ErrClosed)
	}
	defer c.closed.mu.RUnlock()

	req := &snpb.SyncReplicateRequest{
		Destination: replica,
		Payload:     payload,
	}
	_, err := c.rpcClient.SyncReplicate(ctx, req)
	return errors.WithStack(verrors.FromStatusError(err))
}

func (c *client) Close() (err error) {
	c.closed.mu.Lock()
	defer c.closed.mu.Unlock()
	if c.closed.val {
		return nil
	}
	c.closed.val = true

	err = c.rpcConn.Close()
	c.stopper.Stop()
	c.dispatchers.Wait()
	for c.requestQ.Size() > 0 {
		_ = c.requestQ.Pop()
	}
	c.connector.delClient(c)
	c.logger.Info("stop")
	return err
}
