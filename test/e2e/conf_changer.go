package e2e

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
)

type ConfChanger interface {
	Do(context.Context) error
	Done() <-chan struct{}
	Err() error
	Close()
}

type confChanger struct {
	confChangerOptions
	runner *runner.Runner
	mu     sync.RWMutex
	err    error
	done   chan struct{}
}

func NewConfChanger(opts ...ConfChangerOption) ConfChanger {
	ccOpts := defaultConfChangerOptions
	for _, opt := range opts {
		opt(&ccOpts)
	}

	return &confChanger{
		confChangerOptions: ccOpts,
		runner:             runner.New("changer", zap.NewNop()),
		done:               make(chan struct{}),
	}
}

func (cc *confChanger) waitInterval(ctx context.Context) error {
	timer := time.NewTimer(cc.interval)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}

	return errors.WithStack(ctx.Err())
}

func (cc *confChanger) Do(ctx context.Context) error {
	mctx, _ := cc.runner.WithManagedCancel(ctx)
	err := cc.runner.RunC(mctx, func(ctx context.Context) {
		var err error
		defer func() {
			cc.setErr(err)
		}()

		log.Printf("Wait %v\n", cc.interval)
		if err = cc.waitInterval(ctx); err != nil {
			return
		}

		log.Printf("%s\n", testutil.GetFunctionName(cc.change))
		if err = cc.change(); err != nil {
			return
		}

		log.Printf("%s\n", testutil.GetFunctionName(cc.check))
		if err = cc.check(); err != nil {
			return
		}

		log.Printf("Wait %v\n", cc.interval)
		if err = cc.waitInterval(ctx); err != nil {
			return
		}

		log.Printf("%s\n", testutil.GetFunctionName(cc.recover))
		if err = cc.recover(); err != nil {
			return
		}

		log.Printf("%s\n", testutil.GetFunctionName(cc.recoverCheck))
		if err = cc.recoverCheck(); err != nil {
			return
		}

		log.Printf("Wait %v\n", cc.interval)
		if err = cc.waitInterval(ctx); err != nil {
			return
		}
	})

	return err
}

func (cc *confChanger) Done() <-chan struct{} {
	return cc.done
}

func (cc *confChanger) Err() error {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	if cc.err == nil {
		return nil
	}

	return errors.Wrap(cc.err, "conf change")
}

func (cc *confChanger) Close() {
	cc.runner.Stop()
}

func (cc *confChanger) setErr(err error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	fmt.Printf("Conf Change Complete. err = %+v\n", err)
	cc.err = err
	close(cc.done)
}
