package types

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAtomicGLSN(t *testing.T) {
	Convey("AtomicGLSN should work", t, func() {
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
}

func TestAtomicLLSN(t *testing.T) {
	Convey("AtomicLLSN should work", t, func() {
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
}
