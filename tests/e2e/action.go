package e2e

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
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

	fmt.Printf("\nAction - %s with clients(append:%d, subscribe:%v)\n",
		act.title, act.nrCli, act.nrSub)
	if act.prevf != nil {
		fmt.Printf("%s\n", testutil.GetFunctionName(act.prevf))
		if err := act.prevf(); err != nil {
			return err
		}
	}

	errC := make(chan error, 1)
	mctx, _ := act.runner.WithManagedCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		act.runner.Stop()
		fmt.Printf("Appended: %v entries\n", act.appended)

		wg.Wait()

		if act.postf != nil {
			fmt.Printf("%s\n", testutil.GetFunctionName(act.postf))
			act.postf()
		}
	}()

	for i := 0; i < act.nrCli; i++ {
		wg.Add(1)

		if err := act.runner.RunC(mctx, func(rctx context.Context) {
			defer wg.Done()

			err = act.append(rctx)
			if err != nil {
				select {
				case errC <- errors.Wrap(err, "append"):
				default:
				}
				return
			}
		}); err != nil {
			return err
		}
	}

	for i := 0; i < act.nrSub; i++ {
		wg.Add(1)

		if err := act.runner.RunC(mctx, func(rctx context.Context) {
			defer wg.Done()

			for {
				select {
				case <-rctx.Done():
					fmt.Printf("close ctx\n")
					return
				default:
					err := act.subscribe(rctx)
					if err != nil && rctx.Err() == nil {
						fmt.Printf("subscribe error %v\n", err)
						select {
						case errC <- errors.Wrap(err, "subscribe"):
						default:
						}
						return
					}
				}
			}
		}); err != nil {
			return err
		}
	}

	for i := 0; i < act.nrRepeat; i++ {
		for _, confChanger := range act.confChanger {
			cc := confChanger.Clone()

			err = func() error {
				if err := cc.Do(ctx); err != nil {
					return err
				}
				defer cc.Close()

				select {
				case err = <-errC:
					return err
				case <-cc.Done():
					select {
					case err = <-errC:
						return err
					default:
						return cc.Err()
					}
				}
			}()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (act *action) subscribe(ctx context.Context) error {
	vcli, err := varlog.Open(ctx, act.clusterID, []string{act.mrAddr},
		varlog.WithDenyTTL(5*time.Second),
		varlog.WithOpenTimeout(10*time.Second),
	)

	if err != nil {
		return err
	}
	defer vcli.Close()

	res := vcli.Append(ctx, act.topicID, [][]byte{[]byte("foo")}, varlog.WithRetryCount(5))
	if res.Err != nil {
		return errors.Wrap(res.Err, "append")
	}
	limit := res.Metadata[0].GLSN

	var received atomic.Value
	received.Store(types.InvalidGLSN)
	serrC := make(chan error)
	nopOnNext := func(le varlogpb.LogEntry, err error) {
		if err != nil {
			serrC <- err
			close(serrC)
		} else {
			received.Store(le.GLSN)
		}
	}

	fmt.Printf("[%v] Sub ~%v\n", time.Now(), limit)
	defer fmt.Printf("[%v] Sub ~%v Close\n", time.Now(), limit)

	closer, err := vcli.Subscribe(ctx, act.topicID, types.MinGLSN, limit+types.GLSN(1), nopOnNext)
	if err != nil {
		return err
	}
	defer closer()

	subscribeCheckInterval := 3 * time.Second
	timer := time.NewTimer(subscribeCheckInterval)
	defer timer.Stop()

	prevGLSN := types.InvalidGLSN
Loop:
	for {
		select {
		case err, alive := <-serrC:
			if !alive {
				break Loop
			}

			if err != io.EOF {
				return err
			}
		case <-timer.C:
			cur := received.Load().(types.GLSN)
			fmt.Printf("[%v] Sub ~%v check prev:%v, cur:%v\n", time.Now(), limit, prevGLSN, cur)
			if cur == prevGLSN {
				return fmt.Errorf("subscribe timeout")
			}
			prevGLSN = cur

			timer.Reset(subscribeCheckInterval)
		}
	}

	return nil
}

func (act *action) append(ctx context.Context) error {
	vcli, err := varlog.Open(ctx, act.clusterID, []string{act.mrAddr},
		varlog.WithDenyTTL(5*time.Second),
		varlog.WithOpenTimeout(10*time.Second),
	)

	if err != nil {
		return err
	}
	defer vcli.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			res := vcli.Append(ctx, act.topicID, [][]byte{[]byte("foo")}, varlog.WithRetryCount(5))
			if res.Err != nil {
				return res.Err
			}

			act.setAppendResult(res.Metadata[0].GLSN)
		}
	}
}

func (act *action) setAppendResult(glsn types.GLSN) {
	act.mu.Lock()
	defer act.mu.Unlock()

	if act.appended < glsn {
		act.appended = glsn
	}
}
