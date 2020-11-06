package storagenode

import (
	"context"
	"testing"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

func TestLogStreamReporterServiceGetReport(t *testing.T) {
	Convey("Given a LogStreamReporterService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lsrMock := NewMockLogStreamReporter(ctrl)
		lsrMock.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
		service := NewLogStreamReporterService(lsrMock, nil)

		Convey("When LogStreamReporter.GetReport returns an error", func() {
			lsrMock.EXPECT().GetReport(gomock.Any()).Return(
				types.MinGLSN, map[types.LogStreamID]UncommittedLogStreamStatus{}, verrors.ErrInternal)

			Convey("Then LogStreamReporterService.GetReport should return an error", func() {
				_, err := service.GetReport(context.TODO(), &pbtypes.Empty{})
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
		service := NewLogStreamReporterService(lsrMock, nil)

		Convey("When LogStreamReporter.Commit returns an error", func() {
			lsrMock.EXPECT().Commit(gomock.Any(), gomock.Any(), gomock.Any(),
				gomock.Any()).Return(verrors.ErrInternal)

			Convey("Then LogStreamReporterService.Commit should return an error", func() {
				_, err := service.Commit(context.TODO(),
					&snpb.GlobalLogStreamDescriptor{
						HighWatermark:     types.MinGLSN,
						PrevHighWatermark: types.InvalidGLSN,
						CommitResult: []*snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
							{
								LogStreamID:         types.LogStreamID(0),
								CommittedGLSNOffset: types.MinGLSN,
								CommittedGLSNLength: 1,
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
