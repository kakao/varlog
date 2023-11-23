package mrc

import (
	"context"
	"errors"
	"testing"

	pbtypes "github.com/gogo/protobuf/types"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/mock/gomock"
	_ "go.uber.org/mock/mockgen/model"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/mrpb/mock"
)

func TestMRManagementClientGetClusterInfo(t *testing.T) {
	Convey("Given that a ManagementClient calls GetClusterInfo to a ManagementService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockManagementClient(ctrl)
		mc := &metadataRepositoryManagementClient{client: mockClient}

		Convey("When the ManagementService returns an error", func() {
			mockClient.EXPECT().GetClusterInfo(gomock.Any(), gomock.Any()).Return(nil, verrors.ErrInternal)
			Convey("Then the ManagementClient should return the error", func() {
				_, err := mc.GetClusterInfo(context.TODO(), types.ClusterID(1))
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the ManagementService succeeds to get cluster info", func() {
			mockClient.EXPECT().GetClusterInfo(gomock.Any(), gomock.Any()).Return(&mrpb.GetClusterInfoResponse{}, nil)
			Convey("Then the ManagementClient should return the metadata", func() {
				_, err := mc.GetClusterInfo(context.TODO(), types.ClusterID(1))
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestMRManagementClientAddPeer(t *testing.T) {
	Convey("Given that a ManagementClient calls AddPeer to a ManagementService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockManagementClient(ctrl)
		mc := &metadataRepositoryManagementClient{client: mockClient}

		Convey("When the length of passed url is zero", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				err := mc.AddPeer(context.TODO(), types.ClusterID(1), types.NodeID(1), "")
				So(errors.Is(err, verrors.ErrInvalid), ShouldBeTrue)
			})
		})

		Convey("When the nodeID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				err := mc.AddPeer(context.TODO(), types.ClusterID(1), types.InvalidNodeID, "http://127.0.0.1:10000")
				So(errors.Is(err, verrors.ErrInvalid), ShouldBeTrue)
			})
		})

		Convey("When the nodeID is not made from url", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				url := "127.0.0.1:10000"
				nodeID := types.NewNodeIDFromURL(url) + types.NodeID(1)
				err := mc.AddPeer(context.TODO(), types.ClusterID(1), nodeID, url)
				So(errors.Is(err, verrors.ErrInvalid), ShouldBeTrue)
			})
		})

		Convey("When the ManagementService returns an error", func() {
			mockClient.EXPECT().AddPeer(gomock.Any(), gomock.Any()).Return(nil, verrors.ErrInternal)
			Convey("Then the ManagementClient should return the error", func() {
				url := "http://127.0.0.1:10000"
				nodeID := types.NewNodeIDFromURL(url)
				err := mc.AddPeer(context.TODO(), types.ClusterID(1), nodeID, url)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the ManagementService succeeds to add the LogStream", func() {
			mockClient.EXPECT().AddPeer(gomock.Any(), gomock.Any()).Return(&pbtypes.Empty{}, nil)
			Convey("Then the ManagementClient should return the path of the LogStream", func() {
				url := "http://127.0.0.1:10000"
				nodeID := types.NewNodeIDFromURL(url)
				err := mc.AddPeer(context.TODO(), types.ClusterID(1), nodeID, url)
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestMRManagementClientRemovePeer(t *testing.T) {
	Convey("Given that a ManagementClient calls RemovePeer to a ManagementService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockManagementClient(ctrl)
		mc := &metadataRepositoryManagementClient{client: mockClient}

		Convey("When the nodeID is invalid", func() {
			Convey("Then the ManagementClient should return an ErrInvalid", func() {
				err := mc.RemovePeer(context.TODO(), types.ClusterID(1), types.InvalidNodeID)
				So(errors.Is(err, verrors.ErrInvalid), ShouldBeTrue)
			})
		})

		Convey("When the ManagementService returns an error", func() {
			mockClient.EXPECT().RemovePeer(gomock.Any(), gomock.Any()).Return(nil, verrors.ErrInternal)
			Convey("Then the ManagementClient should return the error", func() {
				err := mc.RemovePeer(context.TODO(), types.ClusterID(1), types.MinNodeID)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the ManagementService succeeds to add the LogStream", func() {
			mockClient.EXPECT().RemovePeer(gomock.Any(), gomock.Any()).Return(&pbtypes.Empty{}, nil)
			Convey("Then the ManagementClient should return the path of the LogStream", func() {
				err := mc.RemovePeer(context.TODO(), types.ClusterID(1), types.MinNodeID)
				So(err, ShouldBeNil)
			})
		})
	})
}
