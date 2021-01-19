package storagenode

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode -package storagenode -destination replicator_client_mock.go . ReplicatorClient

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/util/syncutil"
	"github.com/kakao/varlog/pkg/util/syncutil/atomicutil"
	"github.com/kakao/varlog/pkg/util/timeutil"
	"github.com/kakao/varlog/proto/snpb"
)

// TODO (jun): add options
const (
	rcRequestCSize    = 0
	rcRequestCTimeout = timeutil.MaxDuration
)

var errNotRunning = errors.New("replicatorclient: not running")

type ReplicatorClient interface {
	Run(ctx context.Context) error
	Close() error
	Replicate(ctx context.Context, llsn types.LLSN, data []byte) <-chan error
	PeerStorageNodeID() types.StorageNodeID
	SyncReplicate(ctx context.Context, logStreamID types.LogStreamID, first, last, current snpb.SyncPosition, data []byte) error
}

type replicatorClient struct {
	peerStorageNodeID types.StorageNodeID
	peerLogStreamID   types.LogStreamID

	rpcConn   *rpc.Conn
	rpcClient snpb.ReplicatorClient
	stream    snpb.Replicator_ReplicateClient

	once   syncutil.OnlyOnce
	cancel context.CancelFunc

	muErrCs sync.Mutex
	errCs   map[types.LLSN]chan<- error

	requestC chan *snpb.ReplicationRequest

	runner *runner.Runner
	logger *zap.Logger

	running atomicutil.AtomicBool

	onceReplicateStop sync.Once
	replicateStop     chan struct{}
}

// Add more detailed peer info (e.g., storage node id)
func NewReplicatorClient(peerStorageNodeID types.StorageNodeID, peerLogStreamID types.LogStreamID, peerAddress string, logger *zap.Logger) (ReplicatorClient, error) {
	rpcConn, err := rpc.NewBlockingConn(peerAddress)
	if err != nil {
		return nil, err
	}
	return NewReplicatorClientFromRpcConn(peerStorageNodeID, peerLogStreamID, rpcConn, logger)
}

func NewReplicatorClientFromRpcConn(peerStorageNodeID types.StorageNodeID, peerLogStreamID types.LogStreamID, rpcConn *rpc.Conn, logger *zap.Logger) (ReplicatorClient, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("replicatorclient").With(zap.Any("peer_snid", peerStorageNodeID))
	return &replicatorClient{
		peerStorageNodeID: peerStorageNodeID,
		peerLogStreamID:   peerLogStreamID,
		rpcConn:           rpcConn,
		rpcClient:         snpb.NewReplicatorClient(rpcConn.Conn),
		errCs:             make(map[types.LLSN]chan<- error),
		requestC:          make(chan *snpb.ReplicationRequest, rcRequestCSize),
		runner:            runner.New("replicatorclient", logger),
		logger:            logger,
		replicateStop:     make(chan struct{}),
	}, nil
}

func (rc *replicatorClient) PeerStorageNodeID() types.StorageNodeID {
	return rc.peerStorageNodeID
}

func (rc *replicatorClient) Run(ctx context.Context) error {
	return rc.once.Do(func() error {
		mctx, cancel := rc.runner.WithManagedCancel(ctx)
		rc.cancel = cancel

		stream, err := rc.rpcClient.Replicate(mctx)
		if err != nil {
			return errors.Wrap(err, "replicatorclient")
		}
		rc.stream = stream

		if err := rc.runner.RunC(mctx, rc.dispatchRequestC); err != nil {
			return errors.Wrap(err, "replicatorclient")
		}
		if err := rc.runner.RunC(mctx, rc.dispatchResponse); err != nil {
			return errors.Wrap(err, "replicatorclient")
		}
		rc.running.Store(true)
		return nil
	})
}

func (rc *replicatorClient) Close() error {
	rc.running.Store(false)
	if rc.cancel != nil {
		rc.stopReplicate()
		rc.cancel()
		rc.runner.Stop()
		rc.propagateAllError()
	}
	rc.logger.Info("close")
	return rc.rpcConn.Close()
}

func (rc *replicatorClient) stopReplicate() {
	rc.onceReplicateStop.Do(func() {
		close(rc.replicateStop)
	})
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

	req := &snpb.ReplicationRequest{
		LogStreamID: rc.peerLogStreamID,
		LLSN:        llsn,
		Payload:     data,
	}

	rc.muErrCs.Lock()
	rc.errCs[llsn] = errC
	rc.muErrCs.Unlock()

	if err := rc.addRequestC(ctx, req); err != nil {
		rc.propagateError(llsn, err)
	}
	return errC
}

func (rc *replicatorClient) addRequestC(ctx context.Context, req *snpb.ReplicationRequest) error {
	tctx, cancel := context.WithTimeout(ctx, rcRequestCTimeout)
	defer cancel()

	var err error
	select {
	case rc.requestC <- req:
	case <-tctx.Done():
		err = tctx.Err()
	case <-rc.replicateStop:
		err = errNotRunning
	}
	if err == nil {
		rc.logger.Debug("sent ReplicationRequest to requestC", zap.Any("request", req))
	} else {
		rc.logger.Error("stop Replicate", zap.Any("request", req), zap.Error(err))
	}
	return err
}

func (rc *replicatorClient) dispatchRequestC(ctx context.Context) {
LOOP:
	for {
		select {
		case req := <-rc.requestC:
			err := rc.stream.Send(req)
			if err != nil {
				rc.logger.Error("could not send", zap.Error(err), zap.Any("request", req))
				err = errors.Wrap(err, "replicatorclient")
				rc.propagateError(req.GetLLSN(), err)
				break LOOP
			}
		case <-ctx.Done():
			break LOOP
		}
	}

	if err := rc.stream.CloseSend(); err != nil {
		rc.logger.Error("CloseSend error", zap.Error(err))
	}

	rc.stopReplicate()
	rc.cancel()
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
				rc.logger.Info("could not recv", zap.Error(err))
				err = errors.Wrap(err, "replicatorclient")
				break LOOP
			}
			rc.propagateError(rsp.GetLLSN(), err)
		}
	}

	rc.stopReplicate()
	rc.cancel()
	rc.propagateAllError()
}

func (rc *replicatorClient) propagateError(llsn types.LLSN, err error) {
	const (
		errorPropagation = "propagate error"
		okPropagation    = "propagate ok"
	)

	var msg string
	if err == nil {
		msg = okPropagation
	} else {
		msg = errorPropagation
	}

	rc.muErrCs.Lock()
	defer rc.muErrCs.Unlock()
	if errC, ok := rc.errCs[llsn]; ok {
		if ce := rc.logger.Check(zapcore.DebugLevel, msg); ce != nil {
			ce.Write(zap.Any("llsn", llsn), zap.Error(err))
		}
		delete(rc.errCs, llsn)
		errC <- err
		close(errC)
		return
	}
	rc.logger.Error("could not propagate error", zap.Any("llsn", llsn), zap.Error(err))
}

func (rc *replicatorClient) propagateAllError() {
	rc.running.Store(false)
	rc.muErrCs.Lock()
	defer rc.muErrCs.Unlock()
	for llsn, errC := range rc.errCs {
		err := errNotRunning
		rc.logger.Info("propagate error", zap.Any("llsn", llsn), zap.Error(err))
		delete(rc.errCs, llsn)
		errC <- err
		close(errC)
	}
}

func (rc *replicatorClient) SyncReplicate(ctx context.Context, logStreamID types.LogStreamID, first, last, current snpb.SyncPosition, data []byte) error {
	req := &snpb.SyncReplicateRequest{
		First:       first,
		Last:        last,
		Current:     current,
		Data:        data,
		LogStreamID: logStreamID,
	}
	_, err := rc.rpcClient.SyncReplicate(ctx, req)
	return err
}
