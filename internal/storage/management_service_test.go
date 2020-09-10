package storage

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
	"go.uber.org/zap"
)

func TestManagementServiceGetMetadata(t *testing.T) {
	Convey("Given that a ManagementService handles GetMetadata RPC call", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := NewMockManagement(ctrl)
		service := NewManagementService(zap.NewNop(), mock)

		Convey("When the underlying Management failed to get metadata", func() {
			mock.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).Return(nil, varlog.ErrInternal)
			Convey("Then the ManagementService should return an error", func() {
				_, err := service.GetMetadata(context.TODO(), &pb.GetMetadataRequest{})
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the underlying Management succeeds to get metadata", func() {
			mock.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).Return(&vpb.StorageNodeMetadataDescriptor{}, nil)
			Convey("Then the ManagementService should return the metadata", func() {
				_, err := service.GetMetadata(context.TODO(), &pb.GetMetadataRequest{})
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestManagementServiceAddLogStream(t *testing.T) {
	Convey("Given that a ManagementService handles AddLogStream RPC call", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := NewMockManagement(ctrl)
		service := NewManagementService(zap.NewNop(), mock)

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
		service := NewManagementService(zap.NewNop(), mock)

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

func TestManagementServiceSeal(t *testing.T) {
	Convey("Given that a ManagementService handles Seal RPC call", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := NewMockManagement(ctrl)
		service := NewManagementService(zap.NewNop(), mock)

		Convey("When the underlying Management failed to seal the LogStream", func() {
			mock.EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(vpb.LogStreamStatusRunning, types.GLSN(1), varlog.ErrInternal)
			Convey("Then the ManagementService should return an error", func() {
				_, err := service.Seal(context.TODO(), &pb.SealRequest{})
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the underlying Management succeeds to seal the LogStream", func() {
			mock.EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(vpb.LogStreamStatusSealed, types.GLSN(1), nil)
			Convey("Then the ManagementService should not return an error", func() {
				_, err := service.Seal(context.TODO(), &pb.SealRequest{})
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestManagementServiceUnseal(t *testing.T) {
	Convey("Given that a ManagementService handles Unseal RPC call", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := NewMockManagement(ctrl)
		service := NewManagementService(zap.NewNop(), mock)

		Convey("When the underlying Management failed to unseal the LogStream", func() {
			mock.EXPECT().Unseal(gomock.Any(), gomock.Any(), gomock.Any()).Return(varlog.ErrInternal)
			Convey("Then the ManagementService should return an error", func() {
				_, err := service.Unseal(context.TODO(), &pb.UnsealRequest{})
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the underlying Management succeeds to unseal the LogStream", func() {
			mock.EXPECT().Unseal(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			Convey("Then the ManagementService should not return an error", func() {
				_, err := service.Unseal(context.TODO(), &pb.UnsealRequest{})
				So(err, ShouldBeNil)
			})
		})
	})
}
