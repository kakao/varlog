package e2e

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

type Action interface {
	Do(context.Context) error
}

type action struct {
	actionOptions
	appended types.GLSN
	mu       sync.Mutex
	runner   *runner.Runner
}

func NewAction(opts ...ActionOption) Action {
	aOpts := defaultActionOptions
	for _, opt := range opts {
		opt(&aOpts)
	}

	return &action{
		actionOptions: aOpts,
		runner:        runner.New(aOpts.title, aOpts.logger),
	}
}

func (act *action) Do(ctx context.Context) error {
	var err error

	fmt.Printf("\nAction - %s with %d clients\n", act.title, act.nrCli)
	if act.prevf != nil {
		fmt.Printf("%s\n", testutil.GetFunctionName(act.prevf))
		if err := act.prevf(); err != nil {
			return err
		}
	}

	errC := make(chan error, 1)
	mctx, _ := act.runner.WithManagedCancel(ctx)
	defer func() {
		act.runner.Stop()
		fmt.Printf("Appended: %v entries\n", act.appended)
		if act.postf != nil {
			fmt.Printf("%s\n", testutil.GetFunctionName(act.postf))
			act.postf()
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < act.nrCli; i++ {
		wg.Add(1)
		if err := act.runner.RunC(mctx, func(rctx context.Context) {
			varlog, err := varlog.Open(act.clusterID, []string{act.mrAddr}, varlog.WithDenyTTL(5*time.Second))
			wg.Done()

			if err != nil {
				select {
				case errC <- fmt.Errorf("open fail. desc = %s", err.Error()):
				default:
				}
				return
			}
			defer varlog.Close()

			for {
				select {
				case <-rctx.Done():
					return
				default:
					glsn, err := varlog.Append(rctx, []byte("foo"))
					if err != nil {
						select {
						case errC <- fmt.Errorf("append fail. desc = %s", err.Error()):
						default:
						}
						return
					}

					act.setAppendResult(glsn)
				}
			}
		}); err != nil {
			return err
		}
	}

	wg.Wait()

	if err := act.confChanger.Do(ctx); err != nil {
		return err
	}
	defer act.confChanger.Close()

	select {
	case err = <-errC:
		return err
	case <-act.confChanger.Done():
		return act.confChanger.Err()
	}
}

func (act *action) setAppendResult(glsn types.GLSN) {
	act.mu.Lock()
	defer act.mu.Unlock()

	if act.appended < glsn {
		act.appended = glsn
	}
}
