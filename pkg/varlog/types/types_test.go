package types

import (
	"math"
	"math/rand"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/testutil"
)

func TestClusterID(t *testing.T) {
	Convey("ClusterID", t, func() {
		Convey("Too large number", func() { // 64bit processor
			var number uint
			number = math.MaxUint32 + 1
			_, err := NewClusterIDFromUint(number)
			So(err, ShouldNotBeNil)
		})

		Convey("Valid number", func() {
			for i := 0; i < 10000; i++ {
				_, err := NewClusterIDFromUint(uint(rand.Uint32()))
				So(err, ShouldBeNil)
			}
		})
	})
}

func TestStorageNodeID(t *testing.T) {
	Convey("StorageNodeID", t, func() {
		Convey("Too large number", func() { // 64bit processor
			var number uint
			number = math.MaxUint32 + 1
			_, err := NewStorageNodeIDFromUint(number)
			So(err, ShouldNotBeNil)
		})

		Convey("Valid number", func() {
			for i := 0; i < 10000; i++ {
				_, err := NewStorageNodeIDFromUint(uint(rand.Uint32()))
				So(err, ShouldBeNil)
			}
		})

		Convey("Random generator (non-deterministic test)", func() {
			idset := make(map[StorageNodeID]bool)
			for i := 0; i < 10000; i++ {
				var id StorageNodeID
				testutil.CompareWait1(func() bool {
					id = NewStorageNodeID()
					return !idset[id]

				})
				So(idset[id], ShouldBeFalse)
				idset[id] = true
			}
		})
	})
}

func TestGLSN(t *testing.T) {
	Convey("GLSN", t, func() {
		if GLSNLen != 8 {
			t.Fatal("invalid GLSN length")
		}

		var invalid GLSN
		So(invalid.Invalid(), ShouldBeTrue)

		Convey("AtomicGLSN should work", func() {
			glsn := AtomicGLSN(1)
			glsn.Add(10)
			So(glsn.Load(), ShouldEqual, GLSN(11))

			glsn.Store(20)
			So(glsn.Load(), ShouldEqual, GLSN(20))

			So(glsn.CompareAndSwap(GLSN(20), GLSN(21)), ShouldBeTrue)
			So(glsn.Load(), ShouldEqual, GLSN(21))

			So(glsn.CompareAndSwap(GLSN(20), GLSN(22)), ShouldBeFalse)
			So(glsn.Load(), ShouldEqual, GLSN(21))
		})
	})
}

func TestLLSN(t *testing.T) {
	Convey("LLSN", t, func() {
		if LLSNLen != 8 {
			t.Fatal("invalid LLSN length")
		}

		var invalid LLSN
		So(invalid.Invalid(), ShouldBeTrue)

		Convey("AtomicLLSN should work", func() {
			llsn := AtomicLLSN(1)
			llsn.Add(10)
			So(llsn.Load(), ShouldEqual, LLSN(11))

			llsn.Store(20)
			So(llsn.Load(), ShouldEqual, LLSN(20))

			So(llsn.CompareAndSwap(LLSN(20), LLSN(21)), ShouldBeTrue)
			So(llsn.Load(), ShouldEqual, LLSN(21))

			So(llsn.CompareAndSwap(LLSN(20), LLSN(22)), ShouldBeFalse)
			So(llsn.Load(), ShouldEqual, LLSN(21))
		})
	})
}
