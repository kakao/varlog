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
			_, err := s.Read(types.MinGLSN)
			So(err, ShouldNotBeNil)
		})

		Convey("it should not commit unwritten data", func() {
			err := s.Commit(types.MinLLSN, types.MinGLSN)
			So(err, ShouldNotBeNil)
		})

		Convey("it should write data", func() {
			err := s.Write(types.MinLLSN, []byte("log_001"))
			So(err, ShouldBeNil)

			Convey("it should not read uncommitted data", func() {
				_, err := s.Read(types.MinGLSN)
				So(err, ShouldNotBeNil)
			})

			Convey("it should not commit unwritten data", func() {
				err := s.Commit(types.MinLLSN+1, types.MinGLSN)
				So(err, ShouldNotBeNil)
			})

			Convey("it should delete uncommitted data", func() {
				err := s.DeleteUncommitted(types.MinLLSN)
				So(err, ShouldBeNil)

				err = s.Commit(types.MinLLSN, types.GLSN(1))
				So(err, ShouldNotBeNil)
			})

			Convey("it should commit written data", func() {
				err := s.Commit(types.MinLLSN, types.GLSN(2))
				So(err, ShouldBeNil)

				Convey("it should not delete committed data by LLSN", func() {
					err := s.DeleteUncommitted(types.MinLLSN)
					So(err, ShouldNotBeNil)
				})

				Convey("it should read committed data", func() {
					logEntry, err := s.Read(types.GLSN(2))
					So(err, ShouldBeNil)
					So(string(logEntry.Data), ShouldEqual, "log_001")
				})

				Convey("it should not read uncommitted data", func() {
					_, err := s.Read(types.GLSN(1))
					So(err, ShouldNotBeNil)
				})

				Convey("it should delete written data with the same GLSN", func() {
					err := s.DeleteCommitted(types.GLSN(2))
					So(err, ShouldBeNil)

					Convey("it should not read deleted data", func() {
						_, err := s.Read(types.GLSN(2))
						So(err, ShouldNotBeNil)
					})
				})
			})

			Convey("it should not write data at wrong LLSN", func() {
				err := s.Write(types.LLSN(3), []byte("log_003"))
				So(err, ShouldNotBeNil)
			})

			Convey("it sholud write data at next position", func() {
				err := s.Write(types.LLSN(2), []byte("log_002"))
				So(err, ShouldBeNil)

				err = s.Commit(types.LLSN(1), types.GLSN(2))
				So(err, ShouldBeNil)

				err = s.Commit(types.LLSN(2), types.GLSN(4))
				So(err, ShouldBeNil)

				Convey("it should scan log entries", func() {
					scanner, err := s.Scan(types.GLSN(1), types.GLSN(11))
					So(err, ShouldBeNil)

					ent, err := scanner.Next()
					So(err, ShouldBeNil)
					So(ent.LLSN, ShouldEqual, types.LLSN(1))
					So(ent.GLSN, ShouldEqual, types.GLSN(2))
					So(ent.Data, ShouldResemble, []byte("log_001"))

					ent, err = scanner.Next()
					So(err, ShouldBeNil)
					So(ent.LLSN, ShouldEqual, types.LLSN(2))
					So(ent.GLSN, ShouldEqual, types.GLSN(4))
					So(ent.Data, ShouldResemble, []byte("log_002"))

					_, err = scanner.Next()
					So(err, ShouldNotBeNil)
				})
			})
		})
	})
}
