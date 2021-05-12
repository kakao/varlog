package varlog

import (
	"log"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestAllowlistPick(t *testing.T) {
	Convey("Given an allowlist", t, func() {
		const (
			denyTTL        = 10 * time.Second
			expireInterval = 10 * time.Minute // not expire
		)

		allowlist, err := newTransientAllowlist(denyTTL, expireInterval, zap.L())
		So(err, ShouldBeNil)

		Reset(func() {
			So(allowlist.Close(), ShouldBeNil)
		})

		Convey("When it is renewed with empty metadata", func() {
			metadata := &varlogpb.MetadataDescriptor{LogStreams: nil}
			allowlist.Renew(metadata)

			Convey("Then any log stream should not be picked", func() {
				_, picked := allowlist.Pick()
				So(picked, ShouldBeFalse)
			})
		})

		Convey("When it is renewed with not empty metadata", func() {
			metadata := &varlogpb.MetadataDescriptor{
				LogStreams: []*varlogpb.LogStreamDescriptor{
					{LogStreamID: types.LogStreamID(1)},
					{LogStreamID: types.LogStreamID(2)},
				},
			}
			allowlist.Renew(metadata)

			Convey("Then a log stream should be picked", func() {
				_, picked := allowlist.Pick()
				So(picked, ShouldBeTrue)
			})
		})
	})
}

func TestAllowlistDeny(t *testing.T) {
	Convey("Given an allowlist", t, func() {
		const (
			denyTTL        = 10 * time.Second
			expireInterval = 10 * time.Minute // not expire
			logStreamID    = types.LogStreamID(1)
		)

		allowlist, err := newTransientAllowlist(denyTTL, expireInterval, zap.L())
		So(err, ShouldBeNil)

		Reset(func() {
			So(allowlist.Close(), ShouldBeNil)
		})

		Convey("When it has only one log stream that is denied", func() {
			allowlist.Renew(&varlogpb.MetadataDescriptor{
				LogStreams: []*varlogpb.LogStreamDescriptor{
					{LogStreamID: logStreamID},
				},
			})
			allowlist.Deny(logStreamID)

			Convey("Then the log stream should not be picked", func() {
				_, picked := allowlist.Pick()
				So(picked, ShouldBeFalse)
				So(allowlist.Contains(logStreamID), ShouldBeFalse)
			})
		})

		Convey("When it has two log streams that one of them is denied", func() {
			allowlist.Renew(&varlogpb.MetadataDescriptor{
				LogStreams: []*varlogpb.LogStreamDescriptor{
					{LogStreamID: logStreamID},
					{LogStreamID: logStreamID + 1},
				},
			})
			allowlist.Deny(logStreamID + 1)
			So(allowlist.Contains(logStreamID+1), ShouldBeFalse)

			Convey("Then the other should be picked", func() {
				pickedLogStreamID, picked := allowlist.Pick()
				So(picked, ShouldBeTrue)
				So(pickedLogStreamID, ShouldEqual, logStreamID)
				So(allowlist.Contains(logStreamID), ShouldBeTrue)
			})
		})
	})
}

func TestAllowlistExpire(t *testing.T) {
	Convey("Given an allowlist", t, func() {
		const (
			denyTTL        = 100 * time.Millisecond
			expireInterval = 100 * time.Millisecond
			logStreamID    = types.LogStreamID(1)
		)

		metadata := &varlogpb.MetadataDescriptor{
			LogStreams: []*varlogpb.LogStreamDescriptor{
				{LogStreamID: logStreamID},
			},
		}

		allowlist, err := newTransientAllowlist(denyTTL, expireInterval, zap.L())
		So(err, ShouldBeNil)

		Reset(func() {
			So(allowlist.Close(), ShouldBeNil)
		})

		allowlist.Renew(metadata)
		_, picked := allowlist.Pick()
		So(picked, ShouldBeTrue)

		allowlist.Deny(logStreamID)
		_, picked = allowlist.Pick()
		So(picked, ShouldBeFalse)

		// wait for some expiration loops
		log.Println("after deny")
		time.Sleep(3 * time.Second)

		_, picked = allowlist.Pick()
		So(picked, ShouldBeTrue)
	})
}

func BenchmarkAllowlistPick(b *testing.B) {
	const (
		denyTTL        = 1 * time.Second
		expireInterval = 1 * time.Second
		numLogStreams  = 1000
	)

	allowlist, err := newTransientAllowlist(denyTTL, expireInterval, zap.L())
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := allowlist.Close(); err != nil {
			b.Fatal(err)
		}
	}()

	metadata := &varlogpb.MetadataDescriptor{}
	for i := 0; i < numLogStreams; i++ {
		metadata.LogStreams = append(metadata.LogStreams,
			&varlogpb.LogStreamDescriptor{LogStreamID: types.LogStreamID(i + 1)},
		)
	}
	allowlist.Renew(metadata)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, picked := allowlist.Pick(); !picked {
			b.Fatal("pick error")
		}
	}
}
