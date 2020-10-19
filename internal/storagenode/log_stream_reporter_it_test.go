package storagenode

import (
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/testutil/conveyutil"
	"google.golang.org/grpc"
)

// See https://github.com/smartystreets/goconvey/issues/220.
func init() {
	/*
	   The default failure mode is FailureHalts, which causes test execution
	   within a `Convey` block to halt at the first failure. You could use
	   that mode if the test were re-worked to aggregate all results into
	   a collection that was verified after all goroutines have finished.
	   But, as the code stands, you need to use the FailureContinues mode.

	   The following line sets the failure mode for all tests in the package:
	*/

	SetDefaultFailureMode(FailureContinues)
}

func TestLogStreamReporterClientLogStreamReporterService(t *testing.T) {
	Convey("Given that a LogStreamReporterService is running", t, func() {
		const storageNodeID = types.StorageNodeID(0)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lsr := NewMockLogStreamReporter(ctrl)
		service := NewLogStreamReporterService(lsr, nil)

		Convey("And a LogStreamReporterClient calls GetReport", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {
			lsr, err := NewLogStreamReporterClient(addr)
			So(err, ShouldBeNil)

			Reset(func() {
				So(lsr.Close(), ShouldBeNil)
			})

			Convey("When the LogStreamReporterService is timed out", func() {
				Convey("Then the LogStreamReporterClient should return error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})
		}))

		Convey("And a LogStreamReporterClient calls Commit", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {
			lsr, err := NewLogStreamReporterClient(addr)
			So(err, ShouldBeNil)

			Reset(func() {
				So(lsr.Close(), ShouldBeNil)
			})

			Convey("When the LogStreamReporterService is timed out", func() {
				Convey("Then the LogStreamReporterClient should return error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})
		}))
	})
}
