package types

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAtomicGLSN(t *testing.T) {
	Convey("AtomicGLSN should work", t, func() {
		var glsn AtomicGLSN
		glsn.Add(1)
		So(glsn.Load(), ShouldEqual, GLSN(1))

		glsn.Store(2)
		So(glsn.Load(), ShouldEqual, GLSN(2))
	})

}
