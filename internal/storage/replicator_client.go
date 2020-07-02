package storage

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/storage_node"
)

type ReplicatorClient interface {
	Run(ctx context.Context)
	Close() error
	Replicate(ctx context.Context, llsn types.LLSN, data []byte) <-chan error
}

type replicatorClient struct {
	rpcConn   *varlog.RpcConn
	rpcClient pb.ReplicatorServiceClient
	once      sync.Once

	cancel context.CancelFunc

	mu        sync.RWMutex
	m         map[types.LLSN]chan<- error
	stream    pb.ReplicatorService_ReplicateClient
	requestC  chan *pb.ReplicationRequest
	responseC chan *pb.ReplicationResponse
}

func NewReplicatorClient(ctx context.Context, address string) (ReplicatorClient, error) {
	rpcConn, err := varlog.NewRpcConn(address)
	if err != nil {
		return nil, err
	}
	return NewReplicatorClientFromRpcConn(ctx, rpcConn)
}

func NewReplicatorClientFromRpcConn(ctx context.Context, rpcConn *varlog.RpcConn) (ReplicatorClient, error) {
	rpcClient := pb.NewReplicatorServiceClient(rpcConn.Conn)
	stream, err := rpcClient.Replicate(ctx)
	if err != nil {
		return nil, err
	}
	return &replicatorClient{
		rpcConn:   rpcConn,
		rpcClient: rpcClient,
		stream:    stream,
		m:         make(map[types.LLSN]chan<- error),
		requestC:  make(chan *pb.ReplicationRequest),
		responseC: make(chan *pb.ReplicationResponse),
	}, nil
}

func (rc *replicatorClient) Run(ctx context.Context) {
	rc.once.Do(func() {
		ctx, cancel := context.WithCancel(ctx)
		rc.cancel = cancel
		go rc.dispatchRequestC(ctx)
	})
}

func (rc *replicatorClient) Close() error {
	rc.cancel()
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
