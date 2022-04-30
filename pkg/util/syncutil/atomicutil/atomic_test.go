package atomicutil

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/goleak"
)

func TestAtomicBool(t *testing.T) {
	Convey("AtomicBool", t, func() {
		var ab AtomicBool

		Convey("its zero value should be same as bool's", func() {
			var b bool
			So(ab.Load() == b, ShouldBeTrue)
		})

		Convey("it should be set by using store", func() {
			ab.Store(true)
			So(ab.Load(), ShouldBeTrue)

			ab.Store(false)
			So(ab.Load(), ShouldBeFalse)
		})

		Convey("CAS", func() {
			var swapped bool
			swapped = ab.CompareAndSwap(true, false)
			So(swapped, ShouldBeFalse)

			swapped = ab.CompareAndSwap(false, true)
			So(swapped, ShouldBeTrue)

			swapped = ab.CompareAndSwap(false, true)
			So(swapped, ShouldBeFalse)

			swapped = ab.CompareAndSwap(true, false)
			So(swapped, ShouldBeTrue)
		})
	})
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
