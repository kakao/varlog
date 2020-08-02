package storage

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog"
	pb "github.com/kakao/varlog/proto/storage_node"
)

func TestManagementServiceAddLogStream(t *testing.T) {
	Convey("Given that a ManagementService handles AddLogStream RPC call", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := NewMockManagement(ctrl)
		service := managementService{m: mock}

		Convey("When the underlying Management failed to add the LogStream", func() {
			mock.EXPECT().AddLogStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("", varlog.ErrInternal)
			Convey("Then the ManagementService should return an error", func() {
				_, err := service.AddLogStream(context.TODO(), &pb.AddLogStreamRequest{})
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the underlying Management is timed out", func() {
			Convey("Then the ManagementService should return a timeout error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed ClusterID is invalid", func() {
			Convey("Then the ManagementService should return an error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed StorageNodeID is invalid", func() {
			Convey("Then the ManagementService should return an error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed LogStreamID is invalid", func() {
			Convey("Then the ManagementService should return an error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the underlying Management succeeds to add the LogStream", func() {
			mock.EXPECT().AddLogStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("/tmp", nil)
			Convey("Then the ManagementService should return a response message about LogStream", func() {
				_, err := service.AddLogStream(context.TODO(), &pb.AddLogStreamRequest{})
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestManagementServiceRemoveLogStream(t *testing.T) {
	Convey("Given that a ManagementService handles RemoveLogStream RPC call", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := NewMockManagement(ctrl)
		service := managementService{m: mock}

		Convey("When the underlying Management failed to remove the LogStream", func() {
			mock.EXPECT().RemoveLogStream(gomock.Any(), gomock.Any(), gomock.Any()).Return(varlog.ErrInternal)
			Convey("Then the ManagementService should return an error", func() {
				_, err := service.RemoveLogStream(context.TODO(), &pb.RemoveLogStreamRequest{})
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the underlying Management is timed out", func() {
			Convey("Then the ManagementService should return a timeout error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed ClusterID is invalid", func() {
			Convey("Then the ManagementService should return an error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed StorageNodeID is invalid", func() {
			Convey("Then the ManagementService should return an error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed LogStreamID is invalid", func() {
			Convey("Then the ManagementService should return an error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the underlying Management succeeds to remove the LogStream", func() {
			mock.EXPECT().RemoveLogStream(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			Convey("Then the ManagementService should not return an error", func() {
				_, err := service.RemoveLogStream(context.TODO(), &pb.RemoveLogStreamRequest{})
				So(err, ShouldBeNil)
			})
		})
	})
}
