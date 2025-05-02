package runner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap/zaptest"
)

func TestRunner(t *testing.T) {
	Convey("Runner", t, func() {
		logger := zaptest.NewLogger(t)
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
			var running atomic.Bool
			running.Store(true)
			worker := func(ctx context.Context) {
				defer running.Store(false)
				<-ctx.Done()
			}
			cancel, err := r.Run(worker)
			So(err, ShouldBeNil)
			So(running.Load(), ShouldBeTrue)
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				assert.Equal(collect, uint64(1), r.NumTasks())
			}, time.Second, 10*time.Millisecond)
			So(len(r.cancels), ShouldEqual, 1)
			cancel()
			So(len(r.cancels), ShouldBeZeroValue)
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				assert.Zero(collect, r.NumTasks())
			}, time.Second, 10*time.Millisecond)
			So(running.Load(), ShouldBeFalse)
		})

		Convey("runner should release resource when task gets into panic", func() {
			var panicHappend atomic.Bool
			cancel, err := r.Run(func(ctx context.Context) {
				defer func() {
					if p := recover(); p != nil {
						panicHappend.Store(true)
					}
				}()
				panic("panic")
			})
			So(err, ShouldBeNil)
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				assert.True(collect, panicHappend.Load())
			}, time.Second, 10*time.Millisecond)
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
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				assert.Equal(collect, uint64(repeat), r.NumTasks())
			}, time.Second, 10*time.Millisecond)
			r.Stop()
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				assert.Zero(collect, r.NumTasks())
			}, time.Second, 10*time.Millisecond)
			So(len(r.cancels), ShouldBeZeroValue)
		})

		Convey("runner should execute tasks with unmanaged context", func() {
			ctx, cancel := context.WithCancel(context.TODO())
			err := r.RunC(ctx, func(ctx context.Context) {
				<-ctx.Done()
			})
			So(err, ShouldBeNil)
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				assert.Equal(collect, uint64(1), r.NumTasks())
			}, time.Second, 10*time.Millisecond)

			Convey("the cancel of the task with unmanaged context should not be added to Runner.cancels", func() {
				So(len(r.cancels), ShouldBeZeroValue)
			})

			cancel()
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				assert.Zero(collect, r.NumTasks())
			}, time.Second, 10*time.Millisecond)
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
				require.EventuallyWithT(t, func(collect *assert.CollectT) {
					assert.Equal(collect, uint64(1), r.NumTasks())
				}, time.Second, 10*time.Millisecond)

				Convey("the task should not increase Runner.cancels", func() {
					So(len(r.cancels), ShouldBeZeroValue)
				})
			})

			var stopped atomic.Bool
			go func() {
				defer stopped.Store(true)
				r.Stop()
			}()
			<-time.Tick(time.Millisecond * 500)
			So(stopped.Load(), ShouldBeFalse)
			So(r.State(), ShouldEqual, Stopping)
			cancel()
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				assert.Zero(collect, r.NumTasks())
				assert.True(collect, stopped.Load())
			}, time.Second, 10*time.Millisecond)
			So(r.State(), ShouldEqual, Stopped)
		})
	})
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
