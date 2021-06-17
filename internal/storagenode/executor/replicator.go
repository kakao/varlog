package executor

//go:generate mockgen -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/executor -package executor -source replicator.go -destination replicator_mock.go -mock_names replicator=MockReplicator

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/replication"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

type replicatorConfig struct {
	queueSize     int
	state         stateProvider
	connectorOpts []replication.ConnectorOption
}

func (c replicatorConfig) validate() error {
	if c.queueSize <= 0 {
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

	// resetConnector closes the underlying connector and recreates a new connector. Closing the
	// underlying connector means that it closes all clients to replicas. The newly created
	// connector has no clients.
	resetConnector() error

	clientOf(ctx context.Context, replica snpb.Replica) (replication.Client, error)
}

type replicatorImpl struct {
	replicatorConfig

	q replicateQueue

	connector replication.Connector

	dispatcher struct {
		runner *runner.Runner
		cancel context.CancelFunc
		mu     sync.Mutex
	}

	// inflight is the number of replicate calls in progress.
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

func (r *replicatorImpl) initConnector() error {
	connector, err := replication.NewConnector(r.connectorOpts...)
	if err != nil {
		return err
	}
	r.connector = connector
	return nil
}

func (r *replicatorImpl) init() error {
	if err := r.initConnector(); err != nil {
		return err
	}

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

	atomic.AddInt64(&r.inflight, int64(len(t.replicas)))

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
	// NOTE: `inflight` should be decreased when the callback is called since all responses
	// // either success and failure should be come before unsealing.
	defer func() {
		atomic.AddInt64(&r.inflight, -1)
	}()
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

	_ = r.connector.Close()
	r.drainQueue()
}

func (r *replicatorImpl) drainQueue() {
	dropped := int64(0)
	for r.q.size() > 0 {
		rt := r.q.pop()
		dropped += int64(len(rt.replicas))
	}
	atomic.AddInt64(&r.inflight, -dropped)
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

func (r *replicatorImpl) resetConnector() error {
	return multierr.Append(r.connector.Close(), r.initConnector())
}

// TODO (jun): Is this good method? If not, replicator can have interface for sync.
func (r *replicatorImpl) clientOf(ctx context.Context, replica snpb.Replica) (replication.Client, error) {
	return r.connector.Get(ctx, replica)
}
