package varlog

import (
	"context"
	"testing"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	"github.daumkakao.com/varlog/varlog/proto/storage_node/mock"
)

func TestManagementClientAddLogStream(t *testing.T) {
	Convey("Given that a ManagementClient calls AddLogStream to a ManagementService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockManagementClient(ctrl)
		mc := &managementClient{rpcClient: mockClient}

		Convey("When the ManagementClient is timed out", func() {
			Convey("Then the ManagementClient should return timeout error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the ManagementService is timed out", func() {
			Convey("Then the ManagementClient should return timeout error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed ClusterID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed StorageNodeID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed LogStreamID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the length of passed path is zero", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				err := mc.AddLogStream(context.TODO(), types.ClusterID(1), types.StorageNodeID(1), types.LogStreamID(1), "")
				So(err, ShouldResemble, ErrInvalid)
			})
		})

		Convey("When the ManagementService returns an error", func() {
			mockClient.EXPECT().AddLogStream(gomock.Any(), gomock.Any()).Return(nil, ErrInternal)
			Convey("Then the ManagementClient should return the error", func() {
				err := mc.AddLogStream(context.TODO(), types.ClusterID(1), types.StorageNodeID(1), types.LogStreamID(1), "/tmp")
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the ManagementService succeeds to add the LogStream", func() {
			mockClient.EXPECT().AddLogStream(gomock.Any(), gomock.Any()).Return(&pb.AddLogStreamResponse{}, nil)
			Convey("Then the ManagementClient should return the path of the LogStream", func() {
				err := mc.AddLogStream(context.TODO(), types.ClusterID(1), types.StorageNodeID(1), types.LogStreamID(1), "/tmp")
				So(err, ShouldBeNil)
				// TODO(jun)
				// Check returned path
			})
		})
	})
}

func TestManagementClientRemoveLogStream(t *testing.T) {
	Convey("Given that a ManagementClient calls RemoveLogStream to a ManagementService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockManagementClient(ctrl)
		mc := &managementClient{rpcClient: mockClient}

		Convey("When the ManagementClient is timed out", func() {
			Convey("Then the ManagementClient should return timeout error", func() {
			})
		})

		Convey("When the ManagementService is timed out", func() {
			Convey("Then the ManagementClient should return timeout error", func() {
			})
		})

		Convey("When the passed ClusterID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed StorageNodeID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed LogStreamID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the ManagementService returns an error", func() {
			mockClient.EXPECT().RemoveLogStream(gomock.Any(), gomock.Any()).Return(nil, ErrInternal)
			Convey("Then the ManagementClient should return the error", func() {
				err := mc.RemoveLogStream(context.TODO(), types.ClusterID(1), types.StorageNodeID(1), types.LogStreamID(1))
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the ManagementService succeeds to remove the LogStream", func() {
			mockClient.EXPECT().RemoveLogStream(gomock.Any(), gomock.Any()).Return(&pbtypes.Empty{}, nil)
			Convey("Then the ManagementClient should not return an error", func() {
				err := mc.RemoveLogStream(context.TODO(), types.ClusterID(1), types.StorageNodeID(1), types.LogStreamID(1))
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestManagementClientSeal(t *testing.T) {
	Convey("Given that a ManagementClient calls Seal to a ManagementService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockManagementClient(ctrl)
		mc := &managementClient{rpcClient: mockClient}

		Convey("When the ManagementClient is timed out", func() {
			Convey("Then the ManagementClient should return timeout error", func() {
			})
		})

		Convey("When the ManagementService is timed out", func() {
			Convey("Then the ManagementClient should return timeout error", func() {
			})
		})

		Convey("When the passed ClusterID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed StorageNodeID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed LogStreamID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the ManagementService returns an error", func() {
			mockClient.EXPECT().Seal(gomock.Any(), gomock.Any()).Return(nil, ErrInternal)
			Convey("Then the ManagementClient should return the error", func() {
				_, _, err := mc.Seal(context.TODO(), types.ClusterID(1), types.StorageNodeID(1), types.LogStreamID(1), types.GLSN(1))
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the ManagementService succeeds to remove the LogStream", func() {
			mockClient.EXPECT().Seal(gomock.Any(), gomock.Any()).Return(&pb.SealResponse{}, nil)
			Convey("Then the ManagementClient should not return an error", func() {
				_, _, err := mc.Seal(context.TODO(), types.ClusterID(1), types.StorageNodeID(1), types.LogStreamID(1), types.GLSN(1))
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestManagementClientUnseal(t *testing.T) {
	Convey("Given that a ManagementClient calls Unseal to a ManagementService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockManagementClient(ctrl)
		mc := &managementClient{rpcClient: mockClient}

		Convey("When the ManagementClient is timed out", func() {
			Convey("Then the ManagementClient should return timeout error", func() {
			})
		})

		Convey("When the ManagementService is timed out", func() {
			Convey("Then the ManagementClient should return timeout error", func() {
			})
		})

		Convey("When the passed ClusterID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed StorageNodeID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the passed LogStreamID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				Convey("This isn't yet implemented", nil)
			})
		})

		Convey("When the ManagementService returns an error", func() {
			mockClient.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(nil, ErrInternal)
			Convey("Then the ManagementClient should return the error", func() {
				err := mc.Unseal(context.TODO(), types.ClusterID(1), types.StorageNodeID(1), types.LogStreamID(1))
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the ManagementService succeeds to remove the LogStream", func() {
			mockClient.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(&pbtypes.Empty{}, nil)
			Convey("Then the ManagementClient should not return an error", func() {
				err := mc.Unseal(context.TODO(), types.ClusterID(1), types.StorageNodeID(1), types.LogStreamID(1))
				So(err, ShouldBeNil)
			})
		})
	})
}
