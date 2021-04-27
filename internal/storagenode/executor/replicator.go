package executor

//go:generate mockgen -self_package github.com/kakao/varlog/internal/storagenode/executor -package executor -source replicator.go -destination replicator_mock.go -mock_names replicator=MockReplicator

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/internal/storagenode/replication"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/verrors"
)

type replicatorConfig struct {
	queueSize int
	connector replication.Connector
	state     stateProvider
}

func (c replicatorConfig) validate() error {
	if c.queueSize <= 0 {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if c.connector == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if c.state == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	return nil
}

type replicator interface {
	send(ctx context.Context, t *replicateTask) error
	stop()
	waitForDrainage(ctx context.Context) error
}

type replicatorImpl struct {
	replicatorConfig

	q replicateQueue

	dispatcher struct {
		runner *runner.Runner
		cancel context.CancelFunc
		mu     sync.Mutex
	}

	inflight int64

	running struct {
		val bool
		mu  sync.RWMutex
	}

	popCv struct {
		cv *sync.Cond
		mu sync.Mutex
	}
}

func newReplicator(cfg replicatorConfig) (*replicatorImpl, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	r := &replicatorImpl{
		replicatorConfig: cfg,
	}
	if err := r.init(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *replicatorImpl) init() error {
	q, err := newReplicateQueue(r.queueSize)
	if err != nil {
		return err
	}
	r.q = q

	r.popCv.cv = sync.NewCond(&r.popCv.mu)

	rn := runner.New("replicator", nil)
	cancel, err := rn.Run(r.replicateLoop)
	if err != nil {
		return err
	}
	r.dispatcher.runner = rn
	r.dispatcher.cancel = cancel
	r.running.val = true
	return nil
}

func (r *replicatorImpl) send(ctx context.Context, t *replicateTask) error {
	if t == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}

	r.running.mu.RLock()
	defer r.running.mu.RUnlock()
	if !r.running.val {
		return errors.WithStack(verrors.ErrClosed)
	}

	if err := r.state.mutableWithBarrier(); err != nil {
		return err
	}
	defer r.state.releaseBarrier()

	atomic.AddInt64(&r.inflight, 1)

	return r.q.pushWithContext(ctx, t)
}

func (r *replicatorImpl) replicateLoop(ctx context.Context) {
	for ctx.Err() == nil {

		if err := r.replicateLoopInternal(ctx); err != nil {
			r.state.setSealing()
		}

		r.popCv.cv.L.Lock()
		r.popCv.cv.Signal()
		r.popCv.cv.L.Unlock()
	}
}

func (r *replicatorImpl) replicateLoopInternal(ctx context.Context) error {
	rt, err := r.q.popWithContext(ctx)
	if err != nil {
		return err
	}

	defer func() {
		atomic.AddInt64(&r.inflight, -1)
	}()

	if err := r.replicate(ctx, rt); err != nil {
		return err
	}
	return nil
}

func (r *replicatorImpl) replicate(ctx context.Context, t *replicateTask) error {
	defer t.release()

	replicas := t.replicas
	llsn := t.llsn
	data := t.data

	grp, ctx := errgroup.WithContext(ctx)
	for i := range replicas {
		replica := replicas[i]
		grp.Go(func() error {
			cl, err := r.connector.Get(ctx, replica)
			if err != nil {
				return err
			}
			// FIXME: return some error?
			cl.Replicate(ctx, llsn, data, r.replicateCallback)
			return nil
		})
	}
	return grp.Wait()
}

func (r *replicatorImpl) replicateCallback(err error) {
	if err != nil {
		r.state.setSealing()
	}
}

func (r *replicatorImpl) stop() {
	r.running.mu.Lock()
	r.running.val = false
	r.running.mu.Unlock()

	r.dispatcher.cancel()
	r.dispatcher.runner.Stop()

	r.drainQueue()
}

func (r *replicatorImpl) drainQueue() {
	for r.q.size() > 0 {
		_ = r.q.pop()
	}
}

func (r *replicatorImpl) waitForDrainage(ctx context.Context) error {
	done := make(chan struct{})
	var wg sync.WaitGroup

	defer func() {
		close(done)
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			r.popCv.cv.L.Lock()
			r.popCv.cv.Signal()
			r.popCv.cv.L.Unlock()
		case <-done:
		}
	}()

	r.popCv.cv.L.Lock()
	defer r.popCv.cv.L.Unlock()
	for atomic.LoadInt64(&r.inflight) > 0 && ctx.Err() == nil {
		r.popCv.cv.Wait()
	}
	if r.q.size() == 0 {
		return nil
	}
	return ctx.Err()
}
