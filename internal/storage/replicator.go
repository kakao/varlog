package storage

import (
	"context"
	"errors"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/runner"
	"go.uber.org/zap"
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

var errNoReplicas = errors.New("replicator: no replicas")

type Replicator interface {
	Run(context.Context)
	Close()
	Replicate(context.Context, types.LLSN, []byte, []Replica) <-chan error
}

type replicator struct {
	rcm        map[types.StorageNodeID]ReplicatorClient
	mtxRcm     sync.RWMutex
	replicateC chan *replicateTask
	once       sync.Once
	cancel     context.CancelFunc
	muCancel   sync.Mutex
	runner     *runner.Runner
	logger     *zap.Logger
}

func NewReplicator(logger *zap.Logger) Replicator {
	return &replicator{
		rcm:        make(map[types.StorageNodeID]ReplicatorClient),
		replicateC: make(chan *replicateTask, replicateCSize),
		runner:     runner.New("replicator", logger),
		logger:     logger,
	}
}

func (r *replicator) Run(ctx context.Context) {
	r.once.Do(func() {
		mctx, cancel := r.runner.WithManagedCancel(ctx)
		r.muCancel.Lock()
		r.cancel = cancel
		r.muCancel.Unlock()
		r.runner.RunC(mctx, r.dispatchReplicateC)
	})
}

func (r *replicator) Close() {
	r.muCancel.Lock()
	defer r.muCancel.Unlock()
	if r.cancel != nil {
		r.cancel()
		r.mtxRcm.Lock()
		for _, rc := range r.rcm {
			rc.Close()
			delete(r.rcm, rc.StorageNodeID())
		}
		r.mtxRcm.Unlock()
		r.runner.Stop()
	}
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
		delete(r.rcm, errRc.StorageNodeID())
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
