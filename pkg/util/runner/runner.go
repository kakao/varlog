package runner

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

type State int

const (
	Invalid State = iota
	Running
	Stopping
	Stopped
)

type Runner struct {
	name string

	wg sync.WaitGroup

	mu      sync.RWMutex
	taskID  atomic.Uint64
	cancels map[uint64]context.CancelFunc
	state   State

	numTasks atomic.Uint64

	logger *zap.Logger
}

func New(name string, logger *zap.Logger) *Runner {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Runner{
		name:    name,
		cancels: make(map[uint64]context.CancelFunc),
		state:   Running,
		logger:  logger,
	}
}

// WithManagedCancel returns a copy of parent with a new Done channel. The returned context's Done
// channel is closed when the returned cancel function is called or when the parent context's Done
// channel is closed, whichever happens first.
// Moreover, the returned context is managed by runner, thus, if the runner is stopped, the context
// is also canceled.
//
// Canceling this context releases resources associated with it, so code should call cancel as soon
// as the operations running in this Context complete.
func (r *Runner) WithManagedCancel(parent context.Context) (context.Context, context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctx, cancel := context.WithCancel(parent)

	if r.state != Running {
		cancel()
		return ctx, cancel
	}

	taskID := r.taskID.Add(1)
	managedCancel := func() {
		cancel()
		r.mu.Lock()
		delete(r.cancels, taskID)
		r.mu.Unlock()
	}
	r.cancels[taskID] = managedCancel
	return ctx, managedCancel
}

func (r *Runner) RunC(ctx context.Context, f func(context.Context)) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state != Running {
		return fmt.Errorf("runner-%s: %s", r.name, r.state.String())
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.plusTask()
		f(ctx)
		r.minusTask()
	}()
	return nil
}

// Run executes f in a goroutine. It returns context.CancelFunc and error.
func (r *Runner) Run(f func(context.Context)) (context.CancelFunc, error) {
	ctx, cancel := r.WithManagedCancel(context.Background())
	if err := r.RunC(ctx, f); err != nil {
		cancel()
		return nil, err
	}
	return cancel, nil
}

// Stop stops tasks in this runner.
func (r *Runner) Stop() {
	r.mu.Lock()
	state := r.state
	if r.state == Running {
		r.state = Stopping
	}
	r.mu.Unlock()

	if state != Running {
		return
	}

	r.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(r.cancels))
	for _, cancel := range r.cancels {
		cancels = append(cancels, cancel)
	}
	r.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}

	r.wg.Wait()

	r.mu.Lock()
	r.state = Stopped
	r.mu.Unlock()
}

func (r *Runner) State() State {
	if r == nil {
		return Invalid
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *Runner) NumTasks() uint64 {
	if r == nil {
		return 0
	}
	return r.numTasks.Load()
}

func (r *Runner) plusTask() {
	r.numTasks.Add(1)
}

func (r *Runner) minusTask() {
	r.numTasks.Add(^uint64(0))
}

func (r *Runner) String() string {
	if r == nil {
		return "invalid runner"
	}
	numTasks := r.NumTasks()
	return fmt.Sprintf("runner %v: tasks=%v", r.name, numTasks)
}
