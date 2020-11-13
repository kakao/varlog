package first

import (
	"context"
)

type task struct {
	y interface{}
	e error
}

type Thunk func(context.Context, interface{}) (interface{}, error)
type ThunkNoArg func(context.Context) (interface{}, error)

func Run(ctx context.Context, xs []interface{}, f Thunk) (y interface{}, err error) {
	gs := make([]ThunkNoArg, len(xs))
	for i := range xs {
		x := xs[i]
		g := func(ctx context.Context) (interface{}, error) {
			return f(ctx, x)
		}
		gs[i] = g
	}

	return RunF(ctx, gs)
}

func RunF(ctx context.Context, fs []ThunkNoArg) (y interface{}, err error) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := make(chan task)
	for i := range fs {
		go func(i int) {
			f := fs[i]
			r, e := f(cctx)
			select {
			case c <- task{y: r, e: e}:
			case <-cctx.Done():
			}
		}(i)
	}
	for i := 0; i < len(fs); i++ {
		select {
		case <-cctx.Done():
			return nil, cctx.Err()
		case t := <-c:
			err = t.e
			if err == nil {
				y = t.y
				return
			}
		}
	}
	return
}
