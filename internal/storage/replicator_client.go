package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/runner"
	"github.com/kakao/varlog/pkg/varlog/util/syncutil"
	"github.com/kakao/varlog/pkg/varlog/util/syncutil/atomicutil"
	pb "github.com/kakao/varlog/proto/storage_node"
	"go.uber.org/zap"
)

var errNotRunning = errors.New("replicatorclient: not running")

type ReplicatorClient interface {
	Run(ctx context.Context) error
	Close() error
	Replicate(ctx context.Context, llsn types.LLSN, data []byte) <-chan error
	StorageNodeID() types.StorageNodeID
}

type replicatorClient struct {
	storageNodeID types.StorageNodeID
	logStreamID   types.LogStreamID
	rpcConn       *varlog.RpcConn
	rpcClient     pb.ReplicatorServiceClient
	stream        pb.ReplicatorService_ReplicateClient

	once   syncutil.OnlyOnce
	cancel context.CancelFunc

	muErrCs sync.Mutex
	errCs   map[types.LLSN]chan<- error

	// muRequestC: mutex to avoid race between sending data to requestC and closing the
	// requestC - need more concise implementation
	requestC   chan *pb.ReplicationRequest
	muRequestC sync.Mutex

	runner *runner.Runner
	logger *zap.Logger

	running atomicutil.AtomicBool
}

// Add more detailed peer info (e.g., storage node id)
func NewReplicatorClient(storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, address string, logger *zap.Logger) (ReplicatorClient, error) {
	rpcConn, err := varlog.NewRpcConn(address)
	if err != nil {
		return nil, err
	}
	return NewReplicatorClientFromRpcConn(storageNodeID, logStreamID, rpcConn, logger)
}

func NewReplicatorClientFromRpcConn(storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, rpcConn *varlog.RpcConn, logger *zap.Logger) (ReplicatorClient, error) {
	return &replicatorClient{
		storageNodeID: storageNodeID,
		logStreamID:   logStreamID,
		rpcConn:       rpcConn,
		rpcClient:     pb.NewReplicatorServiceClient(rpcConn.Conn),
		errCs:         make(map[types.LLSN]chan<- error),
		requestC:      make(chan *pb.ReplicationRequest),
		runner:        runner.New("replicatorclient", logger),
		logger:        logger,
	}, nil
}

func (rc *replicatorClient) StorageNodeID() types.StorageNodeID {
	return rc.storageNodeID
}

func (rc *replicatorClient) Run(ctx context.Context) error {
	return rc.once.Do(func() error {
		mctx, cancel := rc.runner.WithManagedCancel(ctx)
		rc.cancel = cancel

		stream, err := rc.rpcClient.Replicate(mctx)
		if err != nil {
			return err
		}
		rc.stream = stream

		if err := rc.runner.RunC(mctx, rc.dispatchRequestC); err != nil {
			return err
		}
		if err := rc.runner.RunC(mctx, rc.dispatchResponse); err != nil {
			return err
		}
		rc.running.Store(true)
		return nil
	})
}

func (rc *replicatorClient) Close() error {
	rc.logger.Info("replicatorclient: close", zap.Any("snid", rc.StorageNodeID()))
	rc.running.Store(false)
	if rc.cancel != nil {
		rc.cancel()
		rc.runner.Stop()
		rc.exhaustRequestC()
		rc.propagateAllError()
	}
	return rc.rpcConn.Close()
}

func (rc *replicatorClient) exhaustRequestC() {
	// mutex guard: prevent from sending message to rc.requestC
	rc.muRequestC.Lock()
	defer rc.muRequestC.Unlock()
	if rc.requestC == nil {
		return
	}
	close(rc.requestC)
	cnt := 0
	for req := range rc.requestC {
		cnt++
		rc.logger.Info("replicatorclient: exhaust requestC", zap.Any("request", req), zap.Any("snid", rc.storageNodeID))
	}
	rc.logger.Info("replicatorclient: complete to exhaust requestC", zap.Int("count", cnt), zap.Any("snid", rc.storageNodeID))
	rc.requestC = nil
}

func (rc *replicatorClient) Replicate(ctx context.Context, llsn types.LLSN, data []byte) <-chan error {
	errC := make(chan error, 1)

	// NOTE (jun): If Replicate() is called before calling Run(), Replicate() returns
	// errNotRunning.
	if !rc.running.Load() {
		errC <- errNotRunning
		close(errC)
		return errC
	}

	// mutex guard: prevent from closing rc.requestC
	rc.muRequestC.Lock()
	defer rc.muRequestC.Unlock()

	// check if the requestC is exhausted
	if rc.requestC == nil {
		errC <- errNotRunning
		close(errC)
		return errC
	}

	req := &pb.ReplicationRequest{
		LogStreamID: rc.logStreamID,
		LLSN:        llsn,
		Payload:     data,
	}

	rc.muErrCs.Lock()
	rc.errCs[llsn] = errC
	rc.muErrCs.Unlock()

	// TODO (jun): timeout for enqueueing req into requestC
	select {
	case rc.requestC <- req:
		rc.logger.Debug("replicatorclient: sent ReplicationRequest to requestC", zap.Any("request", req), zap.Any("snid", rc.storageNodeID))
		return errC
	case <-ctx.Done():
	}
	rc.propagateError(llsn, ctx.Err())
	return errC
}

func (rc *replicatorClient) dispatchRequestC(ctx context.Context) {
LOOP:
	for {
		select {
		case req := <-rc.requestC:
			err := rc.stream.Send(req)
			if err != nil {
				rc.logger.Error("replicatorclient: could not send", zap.Error(err), zap.Any("snid", rc.storageNodeID), zap.Any("request", req))
				rc.propagateError(req.GetLLSN(), err)
				break LOOP
			}
		case <-ctx.Done():
			break LOOP
		}
	}

	if err := rc.stream.CloseSend(); err != nil {
		rc.logger.Error("replicatorclient: CloseSend error", zap.Error(err), zap.Any("snid", rc.storageNodeID))
	}
	rc.cancel()
	rc.exhaustRequestC()
	rc.propagateAllError()
}

func (rc *replicatorClient) dispatchResponse(ctx context.Context) {
LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		default:
			rsp, err := rc.stream.Recv()
			if err != nil {
				rc.logger.Info("replicatorclient: could not recv", zap.Error(err), zap.Any("snid", rc.storageNodeID))
				break LOOP
			}
			rc.propagateError(rsp.GetLLSN(), err)
		}
	}

	rc.cancel()
	rc.propagateAllError()
}

func (rc *replicatorClient) propagateError(llsn types.LLSN, err error) {
	rc.muErrCs.Lock()
	defer rc.muErrCs.Unlock()
	if errC, ok := rc.errCs[llsn]; ok {
		rc.logger.Debug("replicatorclient: propagate error", zap.Any("llsn", llsn), zap.Error(err), zap.Any("snid", rc.storageNodeID))
		delete(rc.errCs, llsn)
		errC <- err
		close(errC)
		return
	}
	rc.logger.Error("replicatorclient: could not propagate error", zap.Any("llsn", llsn), zap.Error(err), zap.Any("snid", rc.storageNodeID))
}

func (rc *replicatorClient) propagateAllError() {
	rc.running.Store(false)
	rc.muErrCs.Lock()
	defer rc.muErrCs.Unlock()
	for llsn, errC := range rc.errCs {
		err := errNotRunning
		rc.logger.Info("replicatorclient: propagate error", zap.Any("llsn", llsn), zap.Any("snid", rc.storageNodeID), zap.Error(err))
		delete(rc.errCs, llsn)
		errC <- err
		close(errC)
	}
}
