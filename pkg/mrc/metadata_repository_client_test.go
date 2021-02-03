package mrc

import (
	"context"
	"testing"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	_ "github.com/golang/mock/mockgen/model"
	assert "github.com/smartystreets/assertions"
	. "github.com/smartystreets/goconvey/convey"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/mrpb/mock"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestMRClientGetMetadata(t *testing.T) {
	Convey("Given that a MRClient calls GetMetadata to a MRService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockMetadataRepositoryServiceClient(ctrl)
		mc := &metadataRepositoryClient{client: mockClient}

		Convey("When the MRService returns an error", func() {
			mockClient.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).Return(nil, verrors.ErrInternal)
			Convey("Then the MRClient should return the error", func() {
				_, err := mc.GetMetadata(context.TODO())
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the MRService succeeds to get cluster info", func() {
			mockClient.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).Return(&mrpb.GetMetadataResponse{}, nil)
			Convey("Then the MRClient should return the metadata", func() {
				_, err := mc.GetMetadata(context.TODO())
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestMRClientRegisterStorageNode(t *testing.T) {
	Convey("Given that a MRClient calls RegisterStorageNode to a MRService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockMetadataRepositoryServiceClient(ctrl)
		mc := &metadataRepositoryClient{client: mockClient}

		Convey("When passed StorageNodeDescriptor is nil", func() {
			Convey("Then the MRClient should return an ErrInvalid", func() {
				err := mc.RegisterStorageNode(context.TODO(), nil)
				So(err, assert.ShouldWrap, verrors.ErrInvalid)
			})
		})

		Convey("When passed Address in StorageNodeDescriptor is empty", func() {
			Convey("Then the MRClient should return an ErrInvalid", func() {
				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: types.StorageNodeID(0),
					Storages: []*varlogpb.StorageDescriptor{
						{
							Path:  "path",
							Used:  0,
							Total: 100,
						},
					},
				}
				err := mc.RegisterStorageNode(context.TODO(), sn)
				So(err, assert.ShouldWrap, verrors.ErrInvalid)
			})
		})

		Convey("When passed Storages in StorageNodeDescriptor is empty", func() {
			Convey("Then the MRClient should return an ErrInvalid", func() {
				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: types.StorageNodeID(0),
					Address:       "address",
				}
				err := mc.RegisterStorageNode(context.TODO(), sn)
				So(err, assert.ShouldWrap, verrors.ErrInvalid)
			})
		})

		Convey("When passed Path in StorageDescriptor is empty", func() {
			Convey("Then the MRClient should return an ErrInvalid", func() {
				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: types.StorageNodeID(0),
					Address:       "address",
					Storages: []*varlogpb.StorageDescriptor{
						{
							Used:  0,
							Total: 100,
						},
					},
				}
				err := mc.RegisterStorageNode(context.TODO(), sn)
				So(err, assert.ShouldWrap, verrors.ErrInvalid)
			})
		})

		Convey("When passed Used > Total in StorageDescriptor", func() {
			Convey("Then the MRClient should return an ErrInvalid", func() {
				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: types.StorageNodeID(0),
					Address:       "address",
					Storages: []*varlogpb.StorageDescriptor{
						{
							Path:  "path",
							Used:  10,
							Total: 0,
						},
					},
				}
				err := mc.RegisterStorageNode(context.TODO(), sn)
				So(err, assert.ShouldWrap, verrors.ErrInvalid)
			})
		})

		Convey("When the MRService returns an error", func() {
			mockClient.EXPECT().RegisterStorageNode(gomock.Any(), gomock.Any()).Return(nil, verrors.ErrInternal)
			Convey("Then the MRClient should return the error", func() {
				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: types.StorageNodeID(0),
					Address:       "address",
					Storages: []*varlogpb.StorageDescriptor{
						{
							Path:  "path",
							Used:  0,
							Total: 100,
						},
					},
				}
				So(sn.Valid(), ShouldBeTrue)

				err := mc.RegisterStorageNode(context.TODO(), sn)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the MRService succeeds to register SN", func() {
			mockClient.EXPECT().RegisterStorageNode(gomock.Any(), gomock.Any()).Return(&pbtypes.Empty{}, nil)
			Convey("Then the MRClient should return success", func() {
				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: types.StorageNodeID(0),
					Address:       "address",
					Storages: []*varlogpb.StorageDescriptor{
						{
							Path:  "path",
							Used:  0,
							Total: 100,
						},
					},
				}
				So(sn.Valid(), ShouldBeTrue)

				err := mc.RegisterStorageNode(context.TODO(), sn)
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestMRClientUnregisterStorageNode(t *testing.T) {
	Convey("Given that a MRClient calls UnregisterStorageNode to a MRService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockMetadataRepositoryServiceClient(ctrl)
		mc := &metadataRepositoryClient{client: mockClient}

		Convey("When the MRService returns an error", func() {
			mockClient.EXPECT().UnregisterStorageNode(gomock.Any(), gomock.Any()).Return(nil, verrors.ErrInternal)
			Convey("Then the MRClient should return the error", func() {
				err := mc.UnregisterStorageNode(context.TODO(), types.StorageNodeID(0))
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the MRService succeeds to register SN", func() {
			mockClient.EXPECT().UnregisterStorageNode(gomock.Any(), gomock.Any()).Return(&pbtypes.Empty{}, nil)
			Convey("Then the MRClient should return success", func() {
				err := mc.UnregisterStorageNode(context.TODO(), types.StorageNodeID(0))
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestMRClientRegisterLogStream(t *testing.T) {
	Convey("Given that a MRClient calls RegisterLogStream to a MRService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockMetadataRepositoryServiceClient(ctrl)
		mc := &metadataRepositoryClient{client: mockClient}

		Convey("When passed LogStreamDescriptor is nil", func() {
			Convey("Then the MRClient should return an ErrInvalid", func() {
				err := mc.RegisterLogStream(context.TODO(), nil)
				So(err, assert.ShouldWrap, verrors.ErrInvalid)
			})
		})

		Convey("When passed Replicas in LogStreamDescriptor is empty", func() {
			Convey("Then the MRClient should return an ErrInvalid", func() {
				ls := &varlogpb.LogStreamDescriptor{
					LogStreamID: types.LogStreamID(1),
				}
				err := mc.RegisterLogStream(context.TODO(), ls)
				So(err, assert.ShouldWrap, verrors.ErrInvalid)
			})
		})

		Convey("When passed Path in Replica is empty", func() {
			Convey("Then the MRClient should return an ErrInvalid", func() {
				ls := &varlogpb.LogStreamDescriptor{
					LogStreamID: types.LogStreamID(1),
					Replicas: []*varlogpb.ReplicaDescriptor{
						{
							StorageNodeID: types.StorageNodeID(0),
						},
					},
				}
				So(ls.Valid(), ShouldBeFalse)

				err := mc.RegisterLogStream(context.TODO(), ls)
				So(err, assert.ShouldWrap, verrors.ErrInvalid)
			})
		})

		Convey("When the MRService returns an error", func() {
			mockClient.EXPECT().RegisterLogStream(gomock.Any(), gomock.Any()).Return(nil, verrors.ErrInternal)
			Convey("Then the MRClient should return the error", func() {
				ls := &varlogpb.LogStreamDescriptor{
					LogStreamID: types.LogStreamID(1),
					Replicas: []*varlogpb.ReplicaDescriptor{
						{
							StorageNodeID: types.StorageNodeID(0),
							Path:          "path",
						},
					},
				}
				So(ls.Valid(), ShouldBeTrue)

				err := mc.RegisterLogStream(context.TODO(), ls)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the MRService returns success", func() {
			mockClient.EXPECT().RegisterLogStream(gomock.Any(), gomock.Any()).Return(&pbtypes.Empty{}, nil)
			Convey("Then the MRClient should return success", func() {
				ls := &varlogpb.LogStreamDescriptor{
					LogStreamID: types.LogStreamID(1),
					Replicas: []*varlogpb.ReplicaDescriptor{
						{
							StorageNodeID: types.StorageNodeID(0),
							Path:          "path",
						},
					},
				}
				So(ls.Valid(), ShouldBeTrue)

				err := mc.RegisterLogStream(context.TODO(), ls)
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestMRClientUnregisterLogStream(t *testing.T) {
	Convey("Given that a MRClient calls UnregisterLogStream to a MRService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockMetadataRepositoryServiceClient(ctrl)
		mc := &metadataRepositoryClient{client: mockClient}

		Convey("When the MRService returns an error", func() {
			mockClient.EXPECT().UnregisterLogStream(gomock.Any(), gomock.Any()).Return(nil, verrors.ErrInternal)
			Convey("Then the MRClient should return the error", func() {
				err := mc.UnregisterLogStream(context.TODO(), types.LogStreamID(1))
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the MRService succeeds to register SN", func() {
			mockClient.EXPECT().UnregisterLogStream(gomock.Any(), gomock.Any()).Return(&pbtypes.Empty{}, nil)
			Convey("Then the MRClient should return success", func() {
				err := mc.UnregisterLogStream(context.TODO(), types.LogStreamID(1))
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestMRClientSealLogStream(t *testing.T) {
	Convey("Given that a MRClient calls Seal to a MRService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockMetadataRepositoryServiceClient(ctrl)
		mc := &metadataRepositoryClient{client: mockClient}

		Convey("When the MRService returns an error", func() {
			mockClient.EXPECT().Seal(gomock.Any(), gomock.Any()).Return(nil, verrors.ErrInternal)
			Convey("Then the MRClient should return the error", func() {
				_, err := mc.Seal(context.TODO(), types.LogStreamID(1))
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the MRService succeeds to register SN", func() {
			mockClient.EXPECT().Seal(gomock.Any(), gomock.Any()).Return(&mrpb.SealResponse{}, nil)
			Convey("Then the MRClient should return success", func() {
				_, err := mc.Seal(context.TODO(), types.LogStreamID(1))
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestMRClientUnsealLogStream(t *testing.T) {
	Convey("Given that a MRClient calls Unseal to a MRService", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := mock.NewMockMetadataRepositoryServiceClient(ctrl)
		mc := &metadataRepositoryClient{client: mockClient}

		Convey("When the MRService returns an error", func() {
			mockClient.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(nil, verrors.ErrInternal)
			Convey("Then the MRClient should return the error", func() {
				err := mc.Unseal(context.TODO(), types.LogStreamID(1))
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When the MRService succeeds to register SN", func() {
			mockClient.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(&mrpb.UnsealResponse{}, nil)
			Convey("Then the MRClient should return success", func() {
				err := mc.Unseal(context.TODO(), types.LogStreamID(1))
				So(err, ShouldBeNil)
			})
		})
	})
}
