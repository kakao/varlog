package storage

import (
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/testutil/conveyutil"
	"google.golang.org/grpc"
)

func TestLogStreamReporterClientLogStreamReporterService(t *testing.T) {
	Convey("Given that a LogStreamReporterService is running", t, func() {
		const storageNodeID = types.StorageNodeID(0)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lsr := NewMockLogStreamReporter(ctrl)
		service := NewLogStreamReporterService(lsr)

		Convey("And a LogStreamReporterClient calls GetReport", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {
			_, err := NewLogStreamReporterClient(addr)
			So(err, ShouldBeNil)
			Convey("When the LogStreamReporterService is timed out", func() {
				Convey("Then the LogStreamReporterClient should return error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})
		}))

		Convey("And a LogStreamReporterClient calls Commit", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {
			_, err := NewLogStreamReporterClient(addr)
			So(err, ShouldBeNil)
			Convey("When the LogStreamReporterService is timed out", func() {
				Convey("Then the LogStreamReporterClient should return error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})
		}))
	})
}
