package storage

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog/types"
)

func TestInMemoryStorage(t *testing.T) {
	Convey("InMemoryStorage", t, func() {
		s := NewInMemoryStorage()

		Convey("it should not read unwritten data", func() {
			_, err := s.Read(types.GLSN(10))
			So(err, ShouldNotBeNil)
		})

		Convey("it should not write data with wrong LLSN", func() {
			err := s.Write(types.LLSN(1), nil)
			So(err, ShouldNotBeNil)
		})

		Convey("it should write data", func() {
			err := s.Write(types.LLSN(0), []byte("log_001"))
			So(err, ShouldBeNil)

			Convey("it should not read uncommitted data", func() {
				_, err := s.Read(types.GLSN(10))
				So(err, ShouldNotBeNil)
			})

			Convey("it should commit written data", func() {
				err := s.Commit(types.LLSN(0), types.GLSN(10))
				So(err, ShouldBeNil)

				Convey("it should read committed data", func() {
					data, err := s.Read(types.GLSN(10))
					So(err, ShouldBeNil)
					So(string(data), ShouldEqual, "log_001")

				})

				Convey("it should delete written data with the same GLSN", func() {
					num, err := s.Delete(types.GLSN(10))
					So(err, ShouldBeNil)
					So(num, ShouldEqual, 1)
				})

				Convey("it should delete written data with greater GLSN", func() {
					num, err := s.Delete(types.GLSN(11))
					So(err, ShouldBeNil)
					So(num, ShouldEqual, 1)
				})

				Convey("it should not delete written data with less GLSN", func() {
					num, err := s.Delete(types.GLSN(9))
					So(err, ShouldBeNil)
					So(num, ShouldEqual, 0)
				})
			})

		})
	})
}
