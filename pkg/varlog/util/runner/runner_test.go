package runner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/syncutil/atomicutil"
)

func TestRunner(t *testing.T) {
	Convey("Runner", t, func() {
		var r Runner

		Convey("it should run any worker", func() {
			running := atomicutil.AtomicBool(1)
			worker := func(ctx context.Context) {
				defer running.Store(false)
				select {
				case <-ctx.Done():
				}
			}
			ctx, cancel := context.WithCancel(context.TODO())
			r.Run(ctx, worker)
			So(running.Load(), ShouldBeTrue)
			cancel()
			r.CloseWait()
			So(running.Load(), ShouldBeFalse)
		})

		Convey("it should close workers within a panic", func() {
			panicHappend := atomicutil.AtomicBool(0)
			ctx, cancel := context.WithCancel(context.TODO())
			r.Run(ctx, func(ctx context.Context) {
				defer func() {
					if p := recover(); p != nil {
						t.Logf("recover: %v", p)
						panicHappend.Store(true)
					}
				}()
				panic("panic")
			})
			cancel()
			r.CloseWait()
			So(panicHappend.Load(), ShouldBeTrue)
		})

		Convey("it should close multiple workers with cancel", func() {
			ctx, cancel := context.WithCancel(context.TODO())
			repeat := 100
			var cnt int32 = 0
			for i := 0; i < repeat; i++ {
				r.Run(ctx, func(ctx context.Context) {
					defer atomic.AddInt32(&cnt, 1)
					select {
					case <-ctx.Done():
					}
				})
			}
			cancel()
			r.CloseWait()
			So(cnt, ShouldEqual, repeat)
		})

		Convey("it should close multiple workers with timeout", func() {
			ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*10)
			defer cancel()
			repeat := 100
			var cnt int32 = 0
			for i := 0; i < repeat; i++ {
				r.Run(ctx, func(ctx context.Context) {
					defer atomic.AddInt32(&cnt, 1)
					select {
					case <-ctx.Done():
					}
				})
			}
			r.CloseWait()
			So(cnt, ShouldEqual, repeat)
		})

	})
}
