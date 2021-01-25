package storagenode

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode -package storagenode -destination replicator_client_mock.go . ReplicatorClient

import (
	"context"
	stderrors "errors"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/util/syncutil"
	"github.daumkakao.com/varlog/varlog/pkg/util/syncutil/atomicutil"
	"github.daumkakao.com/varlog/varlog/pkg/util/timeutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

// TODO (jun): add options
const (
	rcRequestCSize    = 0
	rcRequestCTimeout = timeutil.MaxDuration
)

var errNotRunning = stderrors.New("replicatorclient: not running")

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
			return errors.WithMessage(err, "replicatorclient")
		}
		if err := rc.runner.RunC(mctx, rc.dispatchResponse); err != nil {
			return errors.WithMessage(err, "replicatorclient")
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
	defer rc.logger.Info("stop")
	return rc.rpcConn.Close()
}

func (rc *replicatorClient) stopReplicate() {
	rc.onceReplicateStop.Do(func() {
		rc.logger.Info("stopping channels")
		close(rc.replicateStop)
	})
}

func (rc *replicatorClient) Replicate(ctx context.Context, llsn types.LLSN, data []byte) <-chan error {
	errC := make(chan error, 1)

	// NOTE (jun): If Replicate() is called before calling Run(), Replicate() returns
	// errNotRunning.
	if !rc.running.Load() {
		errC <- errors.WithStack(errNotRunning)
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

func (rc *replicatorClient) addRequestC(ctx context.Context, req *snpb.ReplicationRequest) (err error) {
	tctx, cancel := context.WithTimeout(ctx, rcRequestCTimeout)
	defer cancel()

	select {
	case rc.requestC <- req:
	case <-tctx.Done():
		err = errors.Wrap(tctx.Err(), "replicatorclient")
	case <-rc.replicateStop:
		err = errors.WithStack(errNotRunning)
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
				// TODO (jun): Use verrors.FromStatusError
				rc.logger.Error("could not send", zap.Error(err), zap.Any("request", req))
				err = errors.Wrap(err, "replicatorclient: send")
				rc.propagateError(req.GetLLSN(), err)
				break LOOP
			}
		case <-ctx.Done():
			break LOOP
		}
	}

	if err := rc.stream.CloseSend(); err != nil {
		rc.logger.Error("failed to close stream", zap.Error(err))
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
				err = errors.Wrap(err, "replicatorclient: recv")
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
	err = errors.WithMessagef(err, "replicatorclient: lsid = %d, peer_snid = %d, llsn = %d",
		rc.peerLogStreamID,
		rc.peerStorageNodeID,
		llsn)
	rc.muErrCs.Lock()
	defer rc.muErrCs.Unlock()
	if errC, ok := rc.errCs[llsn]; ok {
		delete(rc.errCs, llsn)
		errC <- err
		close(errC)
		return
	}
	rc.logger.DPanic("could not notify an error", zap.Error(err))
}

func (rc *replicatorClient) propagateAllError() {
	rc.running.Store(false)
	rc.muErrCs.Lock()
	defer rc.muErrCs.Unlock()
	err := errors.WithStack(errNotRunning)
	halted := make([]uint64, len(rc.errCs))
	for llsn, errC := range rc.errCs {
		halted = append(halted, uint64(llsn))
		delete(rc.errCs, llsn)
		errC <- err
		close(errC)
	}
	rc.logger.Debug("notify error to all", zap.Uint64s("halted_llsn", halted), zap.Error(err))
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
	return errors.Wrap(verrors.FromStatusError(err), "replicatorclient")
}
