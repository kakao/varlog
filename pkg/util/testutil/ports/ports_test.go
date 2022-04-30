package ports

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/goleak"
)

func TestReserveWeakly(t *testing.T) {
	Convey("Given ports that are not reserved", t, func() {
		Convey("When a free port range is reserved", func() {
			begin := 60000
			leaseThis, errThis := ReserveWeaklyWithRetry(begin)
			So(errThis, ShouldBeNil)
			So(leaseThis.Base(), ShouldBeBetweenOrEqual, begin, MaxPort)

			Convey("Then reserving overlapped range should return an error", func() {
				_, err := ReserveWeakly(leaseThis.Base())
				So(err, ShouldNotBeNil)

				Convey("And reserving overlapped range after releasing should return okay", func() {
					So(leaseThis.Release(), ShouldBeNil)
					lease, err := ReserveWeakly(begin)
					So(err, ShouldBeNil)
					So(lease.Release(), ShouldBeNil)
				})
			})

			Convey("Then reserving another free range should return okay", func() {
				leaseThat, errThat := ReserveWeaklyWithRetry(leaseThis.Base() + ReservationSize)
				So(errThat, ShouldBeNil)
				So(leaseThat.Base(), ShouldBeBetweenOrEqual, begin, MaxPort)
				So(leaseThat.Release(), ShouldBeNil)
				So(leaseThis.Release(), ShouldBeNil)
			})

		})

		Convey("When invalid port range is requested", func() {
			Convey("Then ReserveWeakly should return an error", func() {
				_, err := ReserveWeakly(MinPort - 1)
				So(err, ShouldNotBeNil)

				_, err = ReserveWeakly(MaxPort + 1)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When not aligned port range is requested", func() {
			Convey("Then ReserveWeakly should return an error", func() {
				_, err := ReserveWeakly(10000 + ReservationSize + 1)
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
