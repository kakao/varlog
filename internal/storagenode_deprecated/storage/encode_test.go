package storage

import (
	"testing"

	fuzz "github.com/google/gofuzz"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/kakao/varlog/pkg/types"
)

func TestEncodeDecodeDataKey(t *testing.T) {
	Convey("DataKey", t, func() {
		const n = 1000
		fz := fuzz.New()

		var (
			llsn        types.LLSN
			prevDataKey dataKey
			prevLLSN    types.LLSN
		)
		for i := 0; i < n; i++ {
			fz.Fuzz(&llsn)
			key := encodeDataKey(llsn)
			So(llsn, ShouldEqual, key.decode())

			badKey := dataKey(make([]byte, dataKeyLength))
			copy(badKey, key)
			badKey[0] = commitKeyPrefix
			So(func() { badKey.decode() }, ShouldPanic)

			if llsn != prevLLSN {
				So(key, ShouldNotResemble, prevDataKey)
			}

			prevLLSN = llsn
			prevDataKey = key
		}
	})
}

func TestEncodeDecodeCommitKey(t *testing.T) {
	Convey("CommitKey", t, func() {
		const n = 1000
		fz := fuzz.New()

		var (
			prevCommitKey commitKey
			prevGLSN      types.GLSN
		)
		for i := 0; i < n; i++ {
			var glsn types.GLSN
			fz.Fuzz(&glsn)
			key := encodeCommitKey(glsn)
			So(glsn, ShouldEqual, key.decode())

			badKey := commitKey(make([]byte, commitKeyLength))
			copy(badKey, key)
			badKey[0] = dataKeyPrefix
			So(func() { badKey.decode() }, ShouldPanic)

			if glsn != prevGLSN {
				So(key, ShouldNotResemble, prevCommitKey)
			}

			prevGLSN = glsn
			prevCommitKey = key
		}
	})
}

func TestEncodeDecodeCommitValue(t *testing.T) {
	Convey("CommitValue", t, func() {
		const n = 1000
		fz := fuzz.New()

		for i := 0; i < n; i++ {
			var llsn types.LLSN
			fz.Fuzz(&llsn)
			val := encodeCommitValue(llsn)
			decodedLLSN := val.decode()
			So(llsn, ShouldEqual, decodedLLSN)

			badVal := commitValue(make([]byte, commitValueLength))
			copy(badVal, val)
			badVal[0] = commitKeyPrefix
			So(func() { badVal.decode() }, ShouldPanic)
		}
	})
}

func TestEncodeDecodeCommitContextKey(t *testing.T) {
	Convey("CommitValue", t, func() {
		const n = 1000
		fz := fuzz.New()

		for i := 0; i < n; i++ {
			var cc CommitContext
			fz.Fuzz(&cc)
			key := encodeCommitContextKey(cc)
			decodedCC := key.decode()
			So(cc, ShouldResemble, decodedCC)

			badKey := commitContextKey(make([]byte, commitContextKeyLength))
			copy(badKey, key)
			badKey[0] = commitKeyPrefix
			So(func() { badKey.decode() }, ShouldPanic)
		}
	})
}
