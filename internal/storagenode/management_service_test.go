package storagenode

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestManagementServiceGetMetadata(t *testing.T) {
	Convey("Given that a ManagementService handles GetMetadata RPC call", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := NewMockManagement(ctrl)
		mock.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
		service := NewManagementService(mock, newNopTelmetryStub(), zap.NewNop())

		Convey("When the underlying Management failed to get metadata", func() {
			mock.EXPECT().GetMetadata(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, verrors.ErrInternal)
			Convey("Then the ManagementService should return an error", func() {
				_, err := service.GetMetadata(context.TODO(), &snpb.GetMetadataRequest{})
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the underlying Management succeeds to get metadata", func() {
			mock.EXPECT().GetMetadata(gomock.Any(), gomock.Any(), gomock.Any()).Return(&varlogpb.StorageNodeMetadataDescriptor{}, nil)
			Convey("Then the ManagementService should return the metadata", func() {
				_, err := service.GetMetadata(context.TODO(), &snpb.GetMetadataRequest{})
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
		mock.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
		service := NewManagementService(mock, newNopTelmetryStub(), zap.NewNop())

		Convey("When the underlying Management failed to add the LogStream", func() {
			mock.EXPECT().AddLogStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("", verrors.ErrInternal)
			Convey("Then the ManagementService should return an error", func() {
				_, err := service.AddLogStream(context.TODO(), &snpb.AddLogStreamRequest{})
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
			mock.EXPECT().AddLogStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("/tmp", nil)
			Convey("Then the ManagementService should return a response message about LogStream", func() {
				_, err := service.AddLogStream(context.TODO(), &snpb.AddLogStreamRequest{})
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
		mock.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
		service := NewManagementService(mock, newNopTelmetryStub(), zap.NewNop())

		Convey("When the underlying Management failed to remove the LogStream", func() {
			mock.EXPECT().RemoveLogStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(verrors.ErrInternal)
			Convey("Then the ManagementService should return an error", func() {
				_, err := service.RemoveLogStream(context.TODO(), &snpb.RemoveLogStreamRequest{})
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
			mock.EXPECT().RemoveLogStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			Convey("Then the ManagementService should not return an error", func() {
				_, err := service.RemoveLogStream(context.TODO(), &snpb.RemoveLogStreamRequest{})
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
		mock.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
		service := NewManagementService(mock, newNopTelmetryStub(), zap.NewNop())

		Convey("When the underlying Management failed to seal the LogStream", func() {
			mock.EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(varlogpb.LogStreamStatusRunning, types.GLSN(1), verrors.ErrInternal)
			Convey("Then the ManagementService should return an error", func() {
				_, err := service.Seal(context.TODO(), &snpb.SealRequest{})
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the underlying Management succeeds to seal the LogStream", func() {
			mock.EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(varlogpb.LogStreamStatusSealed, types.GLSN(1), nil)
			Convey("Then the ManagementService should not return an error", func() {
				_, err := service.Seal(context.TODO(), &snpb.SealRequest{})
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
		mock.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
		service := NewManagementService(mock, newNopTelmetryStub(), zap.NewNop())

		Convey("When the underlying Management failed to unseal the LogStream", func() {
			mock.EXPECT().Unseal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(verrors.ErrInternal)
			Convey("Then the ManagementService should return an error", func() {
				_, err := service.Unseal(context.TODO(), &snpb.UnsealRequest{})
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the underlying Management succeeds to unseal the LogStream", func() {
			mock.EXPECT().Unseal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			Convey("Then the ManagementService should not return an error", func() {
				_, err := service.Unseal(context.TODO(), &snpb.UnsealRequest{})
				So(err, ShouldBeNil)
			})
		})
	})
}
