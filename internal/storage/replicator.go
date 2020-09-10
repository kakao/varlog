package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/runner"
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

const (
	replicateCSize = 0
)

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
	runner     runner.Runner
}

func NewReplicator() Replicator {
	return &replicator{
		rcm:        make(map[types.StorageNodeID]ReplicatorClient),
		replicateC: make(chan *replicateTask, replicateCSize),
	}
}

func (r *replicator) Run(ctx context.Context) {
	r.once.Do(func() {
		ctx, cancel := context.WithCancel(ctx)
		r.muCancel.Lock()
		r.cancel = cancel
		r.muCancel.Unlock()
		r.runner.Run(ctx, r.dispatchReplicateC)
	})
}

func (r *replicator) Close() {
	r.muCancel.Lock()
	defer r.muCancel.Unlock()
	if r.cancel != nil {
		r.cancel()
		for _, rc := range r.rcm {
			rc.Close()
		}
		r.runner.CloseWait()
	}
}

func (r *replicator) Replicate(ctx context.Context, llsn types.LLSN, data []byte, replicas []Replica) <-chan error {
	errC := make(chan error, 1)
	if len(replicas) == 0 {
		errC <- fmt.Errorf("no replicas")
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
	errCs := make([]<-chan error, len(t.replicas))
	for i, replica := range t.replicas {
		rc, err := r.getOrConnect(ctx, replica)
		if err != nil {
			t.errC <- err
			close(t.errC)
			return
		}
		errCs[i] = rc.Replicate(ctx, t.llsn, t.data)
	}
	for _, errC := range errCs {
		select {
		case err := <-errC:
			if err != nil {
				t.errC <- err
				close(t.errC)
				return
			}
		case <-ctx.Done():
			t.errC <- ctx.Err()
			close(t.errC)
			return
		}
	}
	t.errC <- nil
	close(t.errC)
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
	rc, err := NewReplicatorClient(replica.Address)
	if err != nil {
		return nil, err
	}
	if err = rc.Run(ctx); err != nil {
		return nil, err
	}
	r.rcm[replica.StorageNodeID] = rc
	return rc, nil
}
