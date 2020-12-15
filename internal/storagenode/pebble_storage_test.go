package storagenode

import (
	"testing"

	fuzz "github.com/google/gofuzz"
	. "github.com/smartystreets/goconvey/convey"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func TestPebbleDataKey(t *testing.T) {
	Convey("Encode/decode data key", t, func() {
		fz := fuzz.New()
		var llsn types.LLSN
		for i := 0; i < 100; i++ {
			fz.Fuzz(&llsn)
			dataKey := encodeDataKey(llsn)
			So(llsn, ShouldEqual, decodeDataKey(dataKey))
			So(func() { decodeCommitKey(dataKey) }, ShouldPanic)
		}
	})
}

func TestPebbleCommitKey(t *testing.T) {
	Convey("Encode/decode commit key", t, func() {
		fz := fuzz.New()
		var glsn types.GLSN
		for i := 0; i < 100; i++ {
			fz.Fuzz(&glsn)
			commitKey := encodeCommitKey(glsn)
			So(glsn, ShouldEqual, decodeCommitKey(commitKey))
			So(func() { decodeDataKey(commitKey) }, ShouldPanic)
		}
	})
}
