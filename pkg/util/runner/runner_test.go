package runner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/util/syncutil/atomicutil"
	"github.com/kakao/varlog/pkg/util/testutil"
)

func TestRunner(t *testing.T) {
	Convey("Runner", t, func() {
		logger, _ := zap.NewDevelopment()
		r := New("test-runner", logger)

		Reset(func() {
			r.Stop()
			So(r.State(), ShouldEqual, Stopped)
		})

		Convey("state of runner should be RunnerRunning before calling Stop, and RunnerStopped after calling Stop", func() {
			So(r.State(), ShouldEqual, Running)
			r.Stop()
			So(r.State(), ShouldEqual, Stopped)
		})

		Convey("state of runner should be RunnerStopped after calling Stop more than two times", func() {
			for i := 0; i < 3; i++ {
				r.Stop()
				So(r.State(), ShouldEqual, Stopped)
			}
		})

		Convey("stopped runner should not run any task", func() {
			r.Stop()
			So(r.State(), ShouldEqual, Stopped)
			_, err := r.Run(func(ctx context.Context) {})
			So(err, ShouldNotBeNil)
		})

		Convey("runner should run a task and release resource of the task when called cancel", func() {
			running := atomicutil.AtomicBool(1)
			worker := func(ctx context.Context) {
				defer running.Store(false)
				<-ctx.Done()
			}
			cancel, err := r.Run(worker)
			So(err, ShouldBeNil)
			So(running.Load(), ShouldBeTrue)
			testutil.CompareWait(func() bool {
				return r.NumTasks() == 1
			}, time.Minute)
			So(len(r.cancels), ShouldEqual, 1)
			cancel()
			So(len(r.cancels), ShouldBeZeroValue)
			testutil.CompareWait(func() bool {
				return r.NumTasks() == 0
			}, time.Minute)
			So(running.Load(), ShouldBeFalse)
		})

		Convey("runner should release resource when task gets into panic", func() {
			var panicHappend atomicutil.AtomicBool
			cancel, err := r.Run(func(ctx context.Context) {
				defer func() {
					if p := recover(); p != nil {
						panicHappend.Store(true)
					}
				}()
				panic("panic")
			})
			So(err, ShouldBeNil)
			testutil.CompareWait(panicHappend.Load, time.Minute)
			cancel()
			So(len(r.cancels), ShouldBeZeroValue)
		})

		Convey("runner should cancel tasks by calling Stop (or CloseWait)", func() {
			repeat := 100
			var cnt int32 = 0
			for i := 0; i < repeat; i++ {
				_, err := r.Run(func(ctx context.Context) {
					defer atomic.AddInt32(&cnt, 1)
					<-ctx.Done()
				})
				So(err, ShouldBeNil)
			}
			r.Stop()
			So(cnt, ShouldEqual, repeat)
			So(len(r.cancels), ShouldBeZeroValue)
		})

		Convey("runner should cancel tasks with managed context when Runner.Stop is called", func() {
			repeat := 100
			for i := 0; i < repeat; i++ {
				ctx, _ := r.WithManagedCancel(context.TODO())
				err := r.RunC(ctx, func(ctx context.Context) {
					<-ctx.Done()
				})
				So(err, ShouldBeNil)
			}
			testutil.CompareWait(func() bool {
				return r.NumTasks() == uint64(repeat)
			}, time.Minute)
			r.Stop()
			testutil.CompareWait(func() bool {
				return r.NumTasks() == 0
			}, time.Minute)
			So(len(r.cancels), ShouldBeZeroValue)
		})

		Convey("runner should execute tasks with unmanaged context", func() {
			ctx, cancel := context.WithCancel(context.TODO())
			err := r.RunC(ctx, func(ctx context.Context) {
				<-ctx.Done()
			})
			So(err, ShouldBeNil)
			testutil.CompareWait(func() bool {
				return r.NumTasks() == 1
			}, time.Minute)

			Convey("the cancel of the task with unmanaged context should not be added to Runner.cancels", func() {
				So(len(r.cancels), ShouldBeZeroValue)
			})

			cancel()
			testutil.CompareWait(func() bool {
				return r.NumTasks() == 0
			}, time.Minute)
			So(ctx.Err(), ShouldResemble, context.Canceled)

			r.Stop()
			So(r.State(), ShouldEqual, Stopped)
		})

		Convey("tasks with unmanaged context should call cancel before the runner is stopped, otherwise Runner.Stop is stuck", func() {
			ctx, cancel := context.WithCancel(context.TODO())
			err := r.RunC(ctx, func(ctx context.Context) {
				<-ctx.Done()
			})
			So(err, ShouldBeNil)

			Convey("the task should increase Runner.NumTask", func() {
				testutil.CompareWait(func() bool {
					return r.NumTasks() == 1
				}, time.Minute)

				Convey("the task should not increase Runner.cancels", func() {
					So(len(r.cancels), ShouldBeZeroValue)
				})
			})

			var stopped atomicutil.AtomicBool
			go func() {
				defer stopped.Store(true)
				r.Stop()
			}()
			<-time.Tick(time.Millisecond * 500)
			So(stopped.Load(), ShouldBeFalse)
			So(r.State(), ShouldEqual, Stopping)
			cancel()
			testutil.CompareWait(func() bool {
				return r.NumTasks() == 0
			}, time.Minute)
			testutil.CompareWait(stopped.Load, time.Minute)
			So(r.State(), ShouldEqual, Stopped)
		})
	})
}
