package syncutil

import (
	"fmt"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestOnlyOnce(t *testing.T) {
	Convey("OnlyOnce", t, func() {
		var onlyonce OnlyOnce
		const repeat = 100

		Convey("should be called only once", func() {
			var wg sync.WaitGroup
			cnt := 0
			wg.Add(repeat)
			for i := 0; i < repeat; i++ {
				go func(o *OnlyOnce, cnt *int) {
					o.Do(func() error {
						*cnt++
						return nil
					})
					wg.Done()
				}(&onlyonce, &cnt)
			}
			wg.Wait()
			So(cnt, ShouldEqual, 1)
		})

		Convey("should be called only once with panic", func() {
			f := func() {
				onlyonce.Do(func() error {
					panic("panic")
				})
			}
			So(f, ShouldPanic)
			i := 0
			onlyonce.Do(func() error {
				i++
				return nil
			})
			So(i, ShouldBeZeroValue)
		})

		Convey("should return result or else value", func() {
			err := onlyonce.Do(func() error {
				return nil
			})
			So(err, ShouldBeNil)
			err = onlyonce.DoOrElse(func() error {
				return nil
			}, func() error {
				return fmt.Errorf("")
			})
			So(err, ShouldNotBeNil)
		})
	})
}
