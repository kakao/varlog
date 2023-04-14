package types

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestClusterID(t *testing.T) {
	Convey("ClusterID", t, func() {
		Convey("Too large number", func() { // 64bit processor
			var number uint = math.MaxUint32 + 1
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

func TestTypesStorageNodeID(t *testing.T) {
	assert.True(t, StorageNodeID(-1).Invalid())
	assert.True(t, StorageNodeID(0).Invalid())
	assert.False(t, StorageNodeID(1).Invalid())
}

func TestGLSN(t *testing.T) {
	Convey("GLSN", t, func() {
		if GLSNLen != 8 {
			t.Fatal("invalid GLSN length")
		}

		var invalid GLSN
		So(invalid.Invalid(), ShouldBeTrue)
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

func TestNodeID(t *testing.T) {
	tcs := []struct {
		input   string
		invalid bool
	}{
		{input: "127.0.0.1", invalid: true},
		{input: ":10000", invalid: true},
		{input: "0:0", invalid: true},
		{input: "0:10000", invalid: true},
		// FIXME: Handle host name.
		{input: "example.com:80", invalid: true},
		// FIXME: Should the wild-card port be valid?
		{input: "127.0.0.1:0", invalid: false},
		{input: "127.0.0.1:10000", invalid: false},
	}

	for _, tc := range tcs {
		t.Run(tc.input, func(t *testing.T) {
			nid := NewNodeID(tc.input)
			if tc.invalid {
				assert.Equal(t, InvalidNodeID, nid)
				return
			}
			assert.NotEqual(t, InvalidNodeID, nid)
			assert.Equal(t, tc.input, nid.Reverse())
		})
	}
}

func TestNodeIDFromURL(t *testing.T) {
	tcs := []struct {
		scheme  string
		addr    string
		invalid bool
	}{
		// Without scheme
		{addr: "127.0.0.1", invalid: true},
		{addr: ":10000", invalid: true},
		{addr: "0:0", invalid: true},
		{addr: "0:10000", invalid: true},
		// FIXME: Handle host name.
		{addr: "example.com:80", invalid: true},
		// FIXME: Should the wild-card port be valid?
		{addr: "127.0.0.1:0", invalid: true},
		{addr: "127.0.0.1:10000", invalid: true},

		// http://
		{scheme: "http", addr: "127.0.0.1", invalid: true},
		{scheme: "http", addr: ":10000", invalid: true},
		{scheme: "http", addr: "0:0", invalid: true},
		{scheme: "http", addr: "0:10000", invalid: true},
		// FIXME: Handle host name.
		{scheme: "http", addr: "example.com:80", invalid: true},
		// FIXME: Should the wild-card port be valid?
		{scheme: "http", addr: "127.0.0.1:0", invalid: false},
		{scheme: "http", addr: "127.0.0.1:10000", invalid: false},

		// https://
		{scheme: "https", addr: "127.0.0.1", invalid: true},
		{scheme: "https", addr: ":10000", invalid: true},
		{scheme: "https", addr: "0:0", invalid: true},
		{scheme: "https", addr: "0:10000", invalid: true},
		// FIXME: Handle host name.
		{scheme: "https", addr: "example.com:80", invalid: true},
		// FIXME: Should the wild-card port be valid?
		{scheme: "https", addr: "127.0.0.1:0", invalid: false},
		{scheme: "https", addr: "127.0.0.1:10000", invalid: false},
	}

	for _, tc := range tcs {
		var sb strings.Builder
		if len(tc.scheme) > 0 {
			fmt.Fprintf(&sb, "%s://", tc.scheme)
		}
		fmt.Fprintf(&sb, "%s", tc.addr)
		url := sb.String()

		t.Run(url, func(t *testing.T) {
			nid := NewNodeIDFromURL(url)
			if tc.invalid {
				assert.Equal(t, InvalidNodeID, nid)
				return
			}
			assert.NotEqual(t, InvalidNodeID, nid)
			assert.Equal(t, tc.addr, nid.Reverse())
		})
	}
}
