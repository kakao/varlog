package runner

import (
	"context"
	"sync"
)

type Runner struct {
	wg sync.WaitGroup
	m  sync.Mutex
}

func (r *Runner) Run(ctx context.Context, f func(context.Context)) {
	r.m.Lock()
	defer r.m.Unlock()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		f(ctx)
	}()
}

func (r *Runner) CloseWait() {
	r.m.Lock()
	defer r.m.Unlock()
	r.wg.Wait()
}
