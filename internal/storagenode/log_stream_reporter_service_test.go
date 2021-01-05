package storagenode

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

func TestLogStreamReporterServiceGetReport(t *testing.T) {
	Convey("Given a LogStreamReporterService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lsrMock := NewMockLogStreamReporter(ctrl)
		lsrMock.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
		service := NewLogStreamReporterService(lsrMock, newNopTelmetryStub(), nil)

		Convey("When LogStreamReporter.GetReport returns an error", func() {
			lsrMock.EXPECT().GetReport(gomock.Any()).Return(
				map[types.LogStreamID]UncommittedLogStreamStatus{}, verrors.ErrInternal)

			Convey("Then LogStreamReporterService.GetReport should return an error", func() {
				_, err := service.GetReport(context.TODO(), &snpb.GetReportRequest{})
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the LogStreamReporter is timed out", func() {
			Convey("Then GetReport should return an error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})
	})
}

func TestLogStreamReporterServiceCommit(t *testing.T) {
	Convey("Given a LogStreamReporterService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lsrMock := NewMockLogStreamReporter(ctrl)
		lsrMock.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
		service := NewLogStreamReporterService(lsrMock, newNopTelmetryStub(), nil)

		Convey("When LogStreamReporter.Commit returns an error", func() {
			lsrMock.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(verrors.ErrInternal)

			Convey("Then LogStreamReporterService.Commit should return an error", func() {
				_, err := service.Commit(context.TODO(),
					&snpb.CommitRequest{
						StorageNodeID: types.StorageNodeID(0),
						CommitResults: []*snpb.LogStreamCommitResult{
							{
								LogStreamID:         types.LogStreamID(0),
								CommittedGLSNOffset: types.MinGLSN,
								CommittedGLSNLength: 1,
								HighWatermark:       types.MinGLSN,
								PrevHighWatermark:   types.InvalidGLSN,
							},
						},
					})
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the LogStreamReporter is timed out", func() {
			Convey("Then Commit should return an error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})
	})
}
