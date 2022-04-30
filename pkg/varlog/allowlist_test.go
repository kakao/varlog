package varlog

import (
	"log"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
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
				_, picked := allowlist.Pick(types.TopicID(1))
				So(picked, ShouldBeFalse)
			})
		})

		Convey("When it is renewed with not empty metadata", func() {
			metadata := &varlogpb.MetadataDescriptor{
				LogStreams: []*varlogpb.LogStreamDescriptor{
					{LogStreamID: types.LogStreamID(1)},
					{LogStreamID: types.LogStreamID(2)},
				},
				Topics: []*varlogpb.TopicDescriptor{
					{
						TopicID: types.TopicID(1),
						LogStreams: []types.LogStreamID{
							types.LogStreamID(1),
							types.LogStreamID(2),
						},
					},
				},
			}
			allowlist.Renew(metadata)

			Convey("Then a log stream should be picked", func() {
				_, picked := allowlist.Pick(types.TopicID(1))
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
			topicID        = types.TopicID(1)
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
				Topics: []*varlogpb.TopicDescriptor{
					{
						TopicID: topicID,
						LogStreams: []types.LogStreamID{
							logStreamID,
						},
					},
				},
			})
			allowlist.Deny(topicID, logStreamID)

			Convey("Then the log stream should not be picked", func() {
				_, picked := allowlist.Pick(topicID)
				So(picked, ShouldBeFalse)
				So(allowlist.Contains(topicID, logStreamID), ShouldBeFalse)
			})
		})

		Convey("When it has two log streams that one of them is denied", func() {
			allowlist.Renew(&varlogpb.MetadataDescriptor{
				LogStreams: []*varlogpb.LogStreamDescriptor{
					{LogStreamID: logStreamID},
					{LogStreamID: logStreamID + 1},
				},
				Topics: []*varlogpb.TopicDescriptor{
					{
						TopicID: topicID,
						LogStreams: []types.LogStreamID{
							logStreamID,
							logStreamID + 1,
						},
					},
				},
			})
			allowlist.Deny(topicID, logStreamID+1)
			So(allowlist.Contains(topicID, logStreamID+1), ShouldBeFalse)

			Convey("Then the other should be picked", func() {
				pickedLogStreamID, picked := allowlist.Pick(topicID)
				So(picked, ShouldBeTrue)
				So(pickedLogStreamID, ShouldEqual, logStreamID)
				So(allowlist.Contains(topicID, logStreamID), ShouldBeTrue)
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
			topicID        = types.TopicID(1)
		)

		metadata := &varlogpb.MetadataDescriptor{
			LogStreams: []*varlogpb.LogStreamDescriptor{
				{LogStreamID: logStreamID},
			},
			Topics: []*varlogpb.TopicDescriptor{
				{
					TopicID: topicID,
					LogStreams: []types.LogStreamID{
						logStreamID,
					},
				},
			},
		}

		allowlist, err := newTransientAllowlist(denyTTL, expireInterval, zap.L())
		So(err, ShouldBeNil)

		Reset(func() {
			So(allowlist.Close(), ShouldBeNil)
		})

		allowlist.Renew(metadata)
		_, picked := allowlist.Pick(topicID)
		So(picked, ShouldBeTrue)

		allowlist.Deny(topicID, logStreamID)
		_, picked = allowlist.Pick(topicID)
		So(picked, ShouldBeFalse)

		// wait for some expiration loops
		log.Println("after deny")
		time.Sleep(3 * time.Second)

		_, picked = allowlist.Pick(topicID)
		So(picked, ShouldBeTrue)
	})
}

func BenchmarkAllowlistPick(b *testing.B) {
	const (
		denyTTL        = 1 * time.Second
		expireInterval = 1 * time.Second
		numLogStreams  = 100
		numTopics      = 100
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
	for i := 0; i < numTopics; i++ {
		topicdesc := &varlogpb.TopicDescriptor{
			TopicID: types.TopicID(i + 1),
		}

		for j := 0; j < numLogStreams; j++ {
			metadata.LogStreams = append(metadata.LogStreams,
				&varlogpb.LogStreamDescriptor{LogStreamID: types.LogStreamID(i*numLogStreams + j + 1)},
			)
			topicdesc.LogStreams = append(topicdesc.LogStreams, types.LogStreamID(i*numLogStreams+j+1))
		}
		metadata.Topics = append(metadata.Topics, topicdesc)
	}

	allowlist.Renew(metadata)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, picked := allowlist.Pick(types.TopicID(i%numTopics + 1)); !picked {
			b.Fatal("pick error")
		}
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
