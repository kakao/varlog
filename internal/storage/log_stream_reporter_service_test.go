package storage

import (
	"context"
	"testing"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
)

func TestLogStreamReporterServiceGetReport(t *testing.T) {
	Convey("Given a LogStreamReporterService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lsrMock := NewMockLogStreamReporter(ctrl)
		lsrMock.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
		service := NewLogStreamReporterService(lsrMock)

		Convey("When LogStreamReporter.GetReport returns an error", func() {
			lsrMock.EXPECT().GetReport(gomock.Any()).Return(
				types.MinGLSN, []UncommittedLogStreamStatus{}, varlog.ErrInternal)

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
		service := NewLogStreamReporterService(lsrMock)

		Convey("When LogStreamReporter.Commit returns an error", func() {
			lsrMock.EXPECT().Commit(gomock.Any(), gomock.Any(), gomock.Any(),
				gomock.Any()).Return(varlog.ErrInternal)

			Convey("Then LogStreamReporterService.Commit should return an error", func() {
				_, err := service.Commit(context.TODO(),
					&pb.GlobalLogStreamDescriptor{
						NextGLSN:     types.MinGLSN,
						PrevNextGLSN: types.InvalidGLSN,
						CommitResult: []*pb.GlobalLogStreamDescriptor_LogStreamCommitResult{
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
