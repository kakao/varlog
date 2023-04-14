package logstream

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/batchlet"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

// replicateClient represents a connection to backup replica for Replicate RPC stream.
type replicateClient struct {
	replicateClientConfig
	queue    chan *replicateTask
	inflight atomic.Int64
	runner   *runner.Runner

	rpcClient    snpb.ReplicatorClient
	streamClient snpb.Replicator_ReplicateClient
	//req          *snpb.ReplicateRequest
}

// newReplicateClient creates a new client to replicate logs to backup replica.
// The argument ctx is used for the client's context. If the client is
// long-running, it should not be canceled during its lifetime.
func newReplicateClient(ctx context.Context, cfg replicateClientConfig) (*replicateClient, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	//rpcConn, err := rpc.NewConn(ctx, cfg.replica.Address, cfg.grpcDialOptions...)
	//if err != nil {
	//	return nil, err
	//}
	rpcClient := snpb.NewReplicatorClient(cfg.rpcConn.Conn)
	streamClient, err := rpcClient.Replicate(ctx)
	if err != nil {
		_ = cfg.rpcConn.Close()
		return nil, err
	}

	rc := &replicateClient{
		replicateClientConfig: cfg,
		queue:                 make(chan *replicateTask, cfg.queueCapacity),
		runner:                runner.New("replicate client", cfg.logger),
		//rpcConn:               rpcConn,
		rpcClient:    rpcClient,
		streamClient: streamClient,
		// NOTE: To reuse the request struct, we need to initialize the field LLSN.
		//req: &snpb.ReplicateRequest{
		//	LLSN: make([]types.LLSN, cfg.lse.maxBatchletLength),
		//},
	}
	if _, err := rc.runner.Run(rc.sendLoop); err != nil {
		_ = rc.rpcConn.Close()
		return nil, err
	}
	return rc, nil
}

// send sends a replicate task to the queue of the replicate client.
func (rc *replicateClient) send(ctx context.Context, rt *replicateTask) (err error) {
	inflight := rc.inflight.Add(1)
	defer func() {
		if err != nil {
			inflight = rc.inflight.Add(-1)
		}
		rc.logger.Debug("sent replicate client a task",
			zap.Int64("inflight", inflight),
			zap.Error(err),
		)
	}()

	switch rc.lse.esm.load() {
	case executorStateSealing, executorStateSealed, executorStateLearning:
		err = verrors.ErrSealed
	case executorStateClosed:
		err = verrors.ErrClosed
	}
	if err != nil {
		return err
	}

	select {
	case rc.queue <- rt:
	case <-ctx.Done():
		err = fmt.Errorf("replicate client: given context canceled: %w", ctx.Err())
	case <-rc.streamClient.Context().Done():
		err = fmt.Errorf("replicate client: stream canceled: %w", rc.streamClient.Context().Err())
	}
	return err
}

// sendLoop is the main loop of the replicate client.
func (rc *replicateClient) sendLoop(ctx context.Context) {
	defer func() {
		_ = rc.streamClient.CloseSend()
	}()
	// NOTE: To reuse the request struct, we need to initialize the field LLSN.
	maxBatchletLength := batchlet.LengthClasses[len(batchlet.LengthClasses)-1]
	req := &snpb.ReplicateRequest{
		TopicID:     rc.lse.tpid,
		LogStreamID: rc.lse.lsid,
		LLSN:        make([]types.LLSN, maxBatchletLength),
	}
	streamCtx := rc.streamClient.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-streamCtx.Done():
			return
		case rt := <-rc.queue:
			if err := rc.sendLoopInternal(ctx, rt, req); err != nil {
				rc.logger.Error("could not send replicated log", zap.Error(err))
				rc.lse.esm.compareAndSwap(executorStateAppendable, executorStateSealing)
				return
			}
		}
	}
}

// sendLoopInternal sends a replicate task to the backup replica.
func (rc *replicateClient) sendLoopInternal(_ context.Context, rt *replicateTask, req *snpb.ReplicateRequest) error {
	// Remove maxAppendSubBatchSize, since rt already has batched data.
	startTime := time.Now()
	// NOTE: We need to copy the LLSN array, since the array is reused.
	req.LLSN = req.LLSN[0:len(rt.llsnList)]
	copy(req.LLSN, rt.llsnList)
	//req.LLSN = rt.llsnList
	req.Data = rt.dataList
	rt.release()
	err := rc.streamClient.Send(req)
	inflight := rc.inflight.Add(-1)
	if rc.lse.lsm != nil {
		rc.lse.lsm.ReplicateClientInflightOperations.Store(inflight)
		rc.lse.lsm.ReplicateClientOperationDuration.Add(time.Since(startTime).Microseconds())
		rc.lse.lsm.ReplicateClientOperations.Add(1)
	}
	return err
}

func (rc *replicateClient) waitForDrainage() {
	const tick = time.Millisecond
	timer := time.NewTimer(tick)
	defer timer.Stop()

	for rc.inflight.Load() > 0 || len(rc.queue) > 0 {
		select {
		case <-timer.C:
			timer.Reset(tick)
		case rt := <-rc.queue:
			rc.inflight.Add(-1)
			rt.release()
		}
	}
}

func (rc *replicateClient) stop() {
	rc.runner.Stop()
	_ = rc.rpcConn.Close()
	rc.waitForDrainage()
}

type replicateClientConfig struct {
	replica varlogpb.LogStreamReplica
	rpcConn *rpc.Conn

	queueCapacity int
	lse           *Executor
	logger        *zap.Logger
}

func (cfg replicateClientConfig) validate() error {
	if err := validateQueueCapacity("replicate client", cfg.queueCapacity); err != nil {
		return fmt.Errorf("replicate client: %w", err)
	}
	if cfg.rpcConn == nil {
		return fmt.Errorf("replicate client: rpc connection is nil")
	}
	if cfg.lse == nil {
		return fmt.Errorf("replicate client: %w", errExecutorIsNil)
	}
	if cfg.logger == nil {
		return fmt.Errorf("replicate client: %w", errLoggerIsNil)
	}
	return nil
}
