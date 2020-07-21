package storage

import (
	"context"
	"testing"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	pb "github.com/kakao/varlog/proto/storage_node"
	"github.com/kakao/varlog/proto/storage_node/mock"
)

func TestLogStreamReporterClientGetReport(t *testing.T) {
	Convey("Given a LogStreamReporterClient", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockLogStreamReporterServiceClient(ctrl)
		lsrc := &logStreamReporterClient{rpcClient: mockClient}

		Convey("When the GetReport RPC is timed out", func() {
			Convey("Then GetReport should return an error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the GetReport RPC succeeds", func() {
			mockClient.EXPECT().GetReport(gomock.Any(), gomock.Any()).Return(&pb.LocalLogStreamDescriptor{}, nil)
			Convey("Then GetReport should return an LocalLogStreamDescriptor", func() {
				_, err := lsrc.GetReport(context.TODO())
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestLogStreamReporterClientCommit(t *testing.T) {
	Convey("Given a LogStreamReporterClient", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockLogStreamReporterServiceClient(ctrl)
		lsrc := &logStreamReporterClient{rpcClient: mockClient}

		Convey("When the Comit RPC is timed out", func() {
			Convey("Then GetReport should return an error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the Commit RPC succeeds", func() {
			mockClient.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(&pbtypes.Empty{}, nil)
			Convey("Then Commit should return nil", func() {
				err := lsrc.Commit(context.TODO(), &pb.GlobalLogStreamDescriptor{})
				So(err, ShouldBeNil)
			})
		})
	})
}
