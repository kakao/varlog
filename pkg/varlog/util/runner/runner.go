package runner

import (
	"context"
	"sync"
)

type Runner struct {
	wg sync.WaitGroup
}

func (r *Runner) Run(ctx context.Context, f func(context.Context)) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		f(ctx)
	}()
}

func (r *Runner) CloseWait() {
	r.wg.Wait()
}
