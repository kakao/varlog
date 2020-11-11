package storagenode

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode -package storagenode -destination replicator_mock.go . Replicator

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/proto/snpb"
)

type Replica struct {
	StorageNodeID types.StorageNodeID
	LogStreamID   types.LogStreamID
	Address       string
}

type replicateTask struct {
	llsn     types.LLSN
	data     []byte
	replicas []Replica

	errC chan<- error
}

const replicateCSize = 0

var (
	errRepKilled  = errors.New("killed replicator")
	errNoReplicas = errors.New("replicator: no replicas")
)

type Replicator interface {
	Run(context.Context) error
	Close()
	Replicate(context.Context, types.LLSN, []byte, []Replica) <-chan error
	SyncReplicate(ctx context.Context, replica Replica, first, last, current snpb.SyncPosition, data []byte) error
}

type replicator struct {
	logStreamID types.LogStreamID
	rcm         map[types.StorageNodeID]ReplicatorClient
	mtxRcm      sync.RWMutex

	running     bool
	muRunning   sync.Mutex
	cancel      context.CancelFunc
	stopped     chan struct{}
	onceStopped sync.Once

	runner     *runner.Runner
	replicateC chan *replicateTask

	logger *zap.Logger
}

func NewReplicator(logStreamID types.LogStreamID, logger *zap.Logger) Replicator {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("replicator")
	return &replicator{
		logStreamID: logStreamID,
		rcm:         make(map[types.StorageNodeID]ReplicatorClient),
		replicateC:  make(chan *replicateTask, replicateCSize),
		runner:      runner.New("replicator", logger),
		stopped:     make(chan struct{}),
		logger:      logger,
	}
}

func (r *replicator) Run(ctx context.Context) error {
	r.muRunning.Lock()
	defer r.muRunning.Unlock()

	if r.running {
		return nil
	}
	r.running = true

	mctx, cancel := r.runner.WithManagedCancel(ctx)
	r.cancel = cancel

	if err := r.runner.RunC(mctx, r.dispatchReplicateC); err != nil {
		cancel()
		r.runner.Stop()
		return err
	}
	r.logger.Info("start")
	return nil
}

func (r *replicator) Close() {
	r.muRunning.Lock()
	defer r.muRunning.Unlock()

	if !r.running {
		return
	}
	r.running = false
	if r.cancel != nil {
		r.cancel()
	}
	r.mtxRcm.Lock()
	for _, rc := range r.rcm {
		rc.Close()
		delete(r.rcm, rc.PeerStorageNodeID())
	}
	r.mtxRcm.Unlock()
	r.runner.Stop()
	r.stop()
	r.logger.Info("close")
}

func (r *replicator) stop() {
	r.onceStopped.Do(func() {
		r.logger.Info("stopped rpc handlers")
		close(r.stopped)
	})
}

func (r *replicator) Replicate(ctx context.Context, llsn types.LLSN, data []byte, replicas []Replica) <-chan error {
	errC := make(chan error, 1)
	if len(replicas) == 0 {
		errC <- errNoReplicas
		close(errC)
		return errC
	}
	task := &replicateTask{
		llsn:     llsn,
		data:     data,
		replicas: replicas,
		errC:     errC,
	}
	select {
	case r.replicateC <- task:
	case <-ctx.Done():
		errC <- ctx.Err()
		close(errC)
	case <-r.stopped:
		errC <- errRepKilled
		close(errC)
	}
	return errC
}

func (r *replicator) dispatchReplicateC(ctx context.Context) {
	for {
		select {
		case t := <-r.replicateC:
			r.replicate(ctx, t)
		case <-ctx.Done():
			return
		}
	}
}

func (r *replicator) replicate(ctx context.Context, t *replicateTask) {
	// prepare replicator clients
	rcs := make([]ReplicatorClient, 0, len(t.replicas))
	for _, replica := range t.replicas {
		rc, err := r.getOrConnect(ctx, replica)
		if err != nil {
			t.errC <- err
			close(t.errC)
			return
		}
		rcs = append(rcs, rc)
	}

	// call Replicate RPC
	errCs := make([]<-chan error, 0, len(rcs))
	for _, rc := range rcs {
		errCs = append(errCs, rc.Replicate(ctx, t.llsn, t.data))
	}

	// errRcs: bad ReplicatorClients
	var err error
	var errRcs []ReplicatorClient
	for i, errC := range errCs {
		select {
		case e := <-errC:
			if e == nil {
				continue
			}
			err = e
		case <-ctx.Done():
			err = ctx.Err()
		}
		errRcs = append(errRcs, rcs[i])
	}
	t.errC <- err
	close(t.errC)

	// close bad ReplicatorClients
	r.mtxRcm.Lock()
	for _, errRc := range errRcs {
		errRc.Close()
		delete(r.rcm, errRc.PeerStorageNodeID())
	}
	r.mtxRcm.Unlock()
}

func (r *replicator) getOrConnect(ctx context.Context, replica Replica) (ReplicatorClient, error) {
	r.mtxRcm.RLock()
	// NOTE: This implies that all of the replicas in a Log Stream is running on different
	// Storage Nodes. Is this a good assumption?
	rc, ok := r.rcm[replica.StorageNodeID]
	r.mtxRcm.RUnlock()
	if ok {
		return rc, nil
	}

	r.mtxRcm.Lock()
	defer r.mtxRcm.Unlock()
	rc, ok = r.rcm[replica.StorageNodeID]
	if ok {
		return rc, nil
	}
	rc, err := NewReplicatorClient(replica.StorageNodeID, replica.LogStreamID, replica.Address, r.logger)
	if err != nil {
		return nil, err
	}
	if err = rc.Run(ctx); err != nil {
		return nil, err
	}
	r.rcm[replica.StorageNodeID] = rc
	return rc, nil
}

func (r *replicator) SyncReplicate(ctx context.Context, replica Replica, first, last, current snpb.SyncPosition, data []byte) error {
	rc, err := r.getOrConnect(ctx, replica)
	if err != nil {
		return err
	}
	return rc.SyncReplicate(ctx, r.logStreamID, first, last, current, data)
}
