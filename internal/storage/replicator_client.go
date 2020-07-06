package storage

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/syncutil"
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
)

type ReplicatorClient interface {
	Run(ctx context.Context) error
	Close() error
	Replicate(ctx context.Context, llsn types.LLSN, data []byte) <-chan error
}

type replicatorClient struct {
	rpcConn   *varlog.RpcConn
	rpcClient pb.ReplicatorServiceClient
	once      syncutil.OnlyOnce
	cancel    context.CancelFunc
	mu        sync.RWMutex
	m         map[types.LLSN]chan<- error
	stream    pb.ReplicatorService_ReplicateClient
	requestC  chan *pb.ReplicationRequest
	responseC chan *pb.ReplicationResponse
}

func NewReplicatorClient(address string) (ReplicatorClient, error) {
	rpcConn, err := varlog.NewRpcConn(address)
	if err != nil {
		return nil, err
	}
	return NewReplicatorClientFromRpcConn(rpcConn)
}

func NewReplicatorClientFromRpcConn(rpcConn *varlog.RpcConn) (ReplicatorClient, error) {
	return &replicatorClient{
		rpcConn:   rpcConn,
		rpcClient: pb.NewReplicatorServiceClient(rpcConn.Conn),
		m:         make(map[types.LLSN]chan<- error),
		requestC:  make(chan *pb.ReplicationRequest),
		responseC: make(chan *pb.ReplicationResponse),
	}, nil
}

func (rc *replicatorClient) Run(ctx context.Context) error {
	return rc.once.Do(func() error {
		ctx, cancel := context.WithCancel(ctx)
		rc.cancel = cancel
		stream, err := rc.rpcClient.Replicate(ctx)
		if err != nil {
			return err
		}
		rc.stream = stream

		go rc.dispatchRequestC(ctx)
		go rc.dispatchResponseC(ctx)
		return nil
	})
}

func (rc *replicatorClient) Close() error {
	if rc.cancel != nil {
		rc.cancel()
	}
	return rc.rpcConn.Close()
}

func (rc *replicatorClient) Replicate(ctx context.Context, llsn types.LLSN, data []byte) <-chan error {
	req := &pb.ReplicationRequest{
		LLSN:    llsn,
		Payload: data,
	}
	errC := make(chan error, 1)
	rc.mu.Lock()
	rc.m[llsn] = errC
	rc.mu.Unlock()

	select {
	case rc.requestC <- req:
		return errC
	case <-ctx.Done():
	}
	rc.propagateError(llsn, ctx.Err())
	return errC
}

func (rc *replicatorClient) dispatchRequestC(ctx context.Context) {
	defer rc.stream.CloseSend()
	defer rc.cancel()
	for {
		select {
		case req := <-rc.requestC:
			err := rc.stream.Send(req)
			if err != nil {
				rc.propagateError(req.GetLLSN(), err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (rc *replicatorClient) dispatchResponseC(ctx context.Context) {
	defer rc.cancel()
LOOP:
	for {
		select {
		case <-ctx.Done():
			return
		default:
			rsp, err := rc.stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				break LOOP
			}
			rc.propagateError(rsp.GetLLSN(), err)
		}
	}
	rc.propagateAllError()
}

func (rc *replicatorClient) propagateError(llsn types.LLSN, err error) {
	rc.mu.Lock()
	errC, ok := rc.m[llsn]
	if ok {
		delete(rc.m, llsn)
	}
	rc.mu.Unlock()
	if !ok {
		panic("no such LLSN")
	}
	errC <- err
	close(errC)
}

func (rc *replicatorClient) propagateAllError() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	for llsn, errC := range rc.m {
		delete(rc.m, llsn)
		errC <- fmt.Errorf("replication channel broken")
		close(errC)
	}
}
