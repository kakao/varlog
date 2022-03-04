package replication

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode_deprecated/replication -package replication -destination client_mock.go . Client

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/kakao/varlog/internal/stopchannel"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/jobqueue"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type Client interface {
	io.Closer
	Replicate(ctx context.Context, llsn types.LLSN, data []byte, startTimeMicro int64, cb func(int64, error))
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
	c.rpcConn, err = rpc.NewConn(ctx, c.replica.GetAddress(), c.grpcDialOptions...)
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

func (c *client) Replicate(ctx context.Context, llsn types.LLSN, data []byte, startTimeMicro int64, callback func(int64, error)) {
	var err error

	c.closed.mu.RLock()
	defer func() {
		c.closed.mu.RUnlock()
		if err != nil {
			callback(startTimeMicro, err)
		}
	}()

	if c.closed.val {
		err = errors.WithStack(verrors.ErrClosed)
		return
	}

	cb := newCallbackBlock(llsn, startTimeMicro, callback)
	err = c.callbackQ.PushWithContext(ctx, cb)
	if err != nil {
		cb.release()
		return
	}

	reqTask := newRequestTask(c.replica.TopicID, c.replica.LogStreamID, llsn, data)
	err = c.requestQ.PushWithContext(ctx, reqTask)
	if err != nil {
		return
	}
	c.metrics.ReplicateClientRequestQueueTasks.Add(ctx, 1)
}

func (c *client) sendLoop(stream snpb.Replicator_ReplicateClient) {
	defer c.dispatchers.Done()

	req := &snpb.ReplicationRequest{}
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
		now := time.Now().UnixMicro()
		reqTask := reqI.(*requestTask)

		c.metrics.ReplicateClientRequestQueueTasks.Add(stream.Context(), -1)
		c.metrics.ReplicateClientRequestQueueTime.Record(
			context.Background(),
			float64(now-reqTask.createdTimeMicro)/1000.0,
		)

		req.TopicID = reqTask.topicID
		req.LogStreamID = reqTask.logStreamID
		req.LLSN = reqTask.llsn
		req.Payload = reqTask.data
		req.CreatedTime = now
		reqTask.release()
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
		rsp = &snpb.ReplicationResponse{}
	)
Loop:
	for {
		// TODO(jun): Is this necessary?
		select {
		case <-stream.Context().Done():
			err = stream.Context().Err()
			break Loop
		default:
		}

		err = stream.RecvMsg(rsp)
		if err != nil {
			break Loop
		}

		c.metrics.ReplicateResponsePropagationTime.Record(
			context.TODO(),
			float64(time.Now().UnixMicro()-rsp.CreatedTime)/1000.0,
		)

		// TODO: Check if this is safe.
		cb := c.callbackQ.Pop().(*callbackBlock)
		if cb.llsn != rsp.GetLLSN() {
			panic(errors.Errorf("llsn mismatch: %d != %d", cb.llsn, rsp.GetLLSN()))
		}
		cb.callback(cb.startTimeMicro, nil)
		cb.release()
	}

	for c.callbackQ.Size() > 0 {
		cb := c.callbackQ.Pop().(*callbackBlock)
		cb.callback(cb.startTimeMicro, err)
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
