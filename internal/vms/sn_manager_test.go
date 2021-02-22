package vms

import (
	"context"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/snc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func withTestStorageNodeManager(t *testing.T, f func(ctrl *gomock.Controller, snManager StorageNodeManager, cmView *MockClusterMetadataView)) func() {
	return func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		meta := &varlogpb.MetadataDescriptor{}
		cmView := NewMockClusterMetadataView(ctrl)
		cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(meta, nil)
		snManager, err := NewStorageNodeManager(context.TODO(), types.ClusterID(1), cmView, zap.L())
		So(err, ShouldBeNil)

		Reset(func() {
			So(snManager.Close(), ShouldBeNil)
		})

		f(ctrl, snManager, cmView)
	}
}

func TestAddStorageNode(t *testing.T) {
	Convey("Given a StorageNodeManager", t, withTestStorageNodeManager(t, func(ctrl *gomock.Controller, snManager StorageNodeManager, cmView *MockClusterMetadataView) {
		Convey("When a StorageNodeID of StorageNode doesn't exist in it", func() {
			snmcl := snc.NewMockStorageNodeManagementClient(ctrl)
			snmcl.EXPECT().Close().Return(nil).AnyTimes()
			snmcl.EXPECT().PeerStorageNodeID().Return(types.StorageNodeID(1)).Times(2)

			Convey("Then the StorageNode should be added to it", func() {

				snManager.AddStorageNode(snmcl)

				Convey("When the StorageNodeID of StorageNode already exists in it", func() {
					Convey("Then the StorageNode should not be added to it", func() {
						f := func() { snManager.AddStorageNode(snmcl) }
						So(f, ShouldPanic)
					})
				})
			})
		})
	}))
}

func TestAddLogStream(t *testing.T) {
	Convey("Given a StorageNodeManager", t, withTestStorageNodeManager(t, func(ctrl *gomock.Controller, snManager StorageNodeManager, cmView *MockClusterMetadataView) {
		const (
			logStreamID = types.LogStreamID(1)
			nrSN        = 3
		)

		logStreamDesc := &varlogpb.LogStreamDescriptor{
			LogStreamID: logStreamID,
			Status:      varlogpb.LogStreamStatusRunning,
		}

		var snmclList []*snc.MockStorageNodeManagementClient

		for snid := types.StorageNodeID(1); snid <= nrSN; snid++ {
			logStreamDesc.Replicas = append(logStreamDesc.Replicas, &varlogpb.ReplicaDescriptor{
				StorageNodeID: snid,
				Path:          "/tmp",
			})

			snmcl := snc.NewMockStorageNodeManagementClient(ctrl)
			snmclList = append(snmclList, snmcl)
			snmcl.EXPECT().PeerStorageNodeID().Return(snid).AnyTimes()
			snmcl.EXPECT().Close().Return(nil).AnyTimes()
		}

		Convey("When a StorageNode doesn't exist", func() {
			// missing SNID=1
			for i := 1; i < len(snmclList); i++ {
				snmcl := snmclList[i]
				snManager.AddStorageNode(snmcl)
			}

			Convey("Then LogStream should not be added", func() {
				cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil)
				err := snManager.AddLogStream(context.TODO(), logStreamDesc)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When at least one of AddLogStream rpc to storage node fails", func() {
			snmclList[0].EXPECT().AddLogStream(gomock.Any(), gomock.Any(), gomock.Any()).Return(verrors.ErrInternal).MaxTimes(1)
			for i := 1; i < len(snmclList); i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().AddLogStream(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).MaxTimes(1)
			}
			for i := 0; i < len(snmclList); i++ {
				snmcl := snmclList[i]
				snManager.AddStorageNode(snmcl)
			}

			Convey("Then LogStream should not be added", func() {
				err := snManager.AddLogStream(context.TODO(), logStreamDesc)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When AddLogStream rpc to StorageNode succeeds", func() {
			for i := 0; i < len(snmclList); i++ {
				snmcl := snmclList[i]
				snManager.AddStorageNode(snmcl)
				snmcl.EXPECT().AddLogStream(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			}

			Convey("Then LogStream should be added", func() {
				err := snManager.AddLogStream(context.TODO(), logStreamDesc)
				So(err, ShouldBeNil)
			})
		})
	}))

}

func TestSeal(t *testing.T) {
	Convey("Given a StorageNodeManager and StorageNodes", t, withTestStorageNodeManager(t, func(ctrl *gomock.Controller, snManager StorageNodeManager, cmView *MockClusterMetadataView) {
		const (
			nrSN        = 3
			logStreamID = types.LogStreamID(1)
		)

		var replicaDescList []*varlogpb.ReplicaDescriptor
		var sndescList []*varlogpb.StorageNodeDescriptor
		var snmclList []*snc.MockStorageNodeManagementClient
		for snid := types.StorageNodeID(1); snid <= nrSN; snid++ {
			snmcl := snc.NewMockStorageNodeManagementClient(ctrl)
			snmcl.EXPECT().PeerStorageNodeID().Return(snid).AnyTimes()
			snmcl.EXPECT().Close().Return(nil).AnyTimes()

			snManager.AddStorageNode(snmcl)

			snmclList = append(snmclList, snmcl)

			sndescList = append(sndescList, &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snid,
				Address:       "127.0.0.1:" + strconv.Itoa(10000+int(snid)),
				Status:        varlogpb.StorageNodeStatusRunning,
				Storages: []*varlogpb.StorageDescriptor{
					{Path: "/tmp", Used: 0, Total: 1},
				},
			})

			replicaDescList = append(replicaDescList, &varlogpb.ReplicaDescriptor{
				StorageNodeID: snid,
				Path:          "/tmp",
			})
		}

		metaDesc := &varlogpb.MetadataDescriptor{
			StorageNodes: sndescList,
			LogStreams: []*varlogpb.LogStreamDescriptor{
				{
					LogStreamID: logStreamID,
					Status:      varlogpb.LogStreamStatusRunning,
					Replicas:    replicaDescList,
				},
			},
		}

		Convey("When getting cluster metadata fails", func() {
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, verrors.ErrInternal).AnyTimes()

			Convey("Then Seal shoud return error", func() {
				_, err := snManager.Seal(context.TODO(), logStreamID, types.MinGLSN)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When Seal rpc to StorageNode fails", func() {
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(metaDesc, nil).AnyTimes()
			const lastGLSN = types.GLSN(10)

			for i := 0; i < len(snmclList)-1; i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().Seal(gomock.Any(), gomock.Any(), lastGLSN).Return(varlogpb.LogStreamStatusSealed, lastGLSN, nil)
			}
			snmclList[len(sndescList)-1].EXPECT().Seal(gomock.Any(), gomock.Any(), lastGLSN).Return(varlogpb.LogStreamStatusRunning, types.InvalidGLSN, verrors.ErrInternal)

			Convey("Then Seal should return response not having the failed node", func() {
				_, err := snManager.Seal(context.TODO(), logStreamID, lastGLSN)
				So(err, ShouldNotBeNil)

				/*
					So(err, ShouldBeNil)
					So(len(lsMetaDescList), ShouldEqual, nrSN-1)

					var snIDs []types.StorageNodeID
					for _, lsMeta := range lsMetaDescList {
						snIDs = append(snIDs, lsMeta.GetStorageNodeID())
					}

					So(snIDs, ShouldContain, types.StorageNodeID(1))
					So(snIDs, ShouldContain, types.StorageNodeID(2))
				*/
			})

		})
	}))
}

func TestUnseal(t *testing.T) {
	Convey("Given a StorageNodeManager and StorageNodes", t, withTestStorageNodeManager(t, func(ctrl *gomock.Controller, snManager StorageNodeManager, cmView *MockClusterMetadataView) {
		const (
			nrSN        = 3
			logStreamID = types.LogStreamID(1)
		)

		var replicaDescList []*varlogpb.ReplicaDescriptor
		var sndescList []*varlogpb.StorageNodeDescriptor
		var snmclList []*snc.MockStorageNodeManagementClient
		for snid := types.StorageNodeID(1); snid <= nrSN; snid++ {
			snmcl := snc.NewMockStorageNodeManagementClient(ctrl)
			snmcl.EXPECT().PeerStorageNodeID().Return(snid).AnyTimes()
			snmcl.EXPECT().Close().Return(nil).AnyTimes()

			snManager.AddStorageNode(snmcl)

			snmclList = append(snmclList, snmcl)

			sndescList = append(sndescList, &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snid,
				Address:       "127.0.0.1:" + strconv.Itoa(10000+int(snid)),
				Status:        varlogpb.StorageNodeStatusRunning,
				Storages: []*varlogpb.StorageDescriptor{
					{Path: "/tmp", Used: 0, Total: 1},
				},
			})

			replicaDescList = append(replicaDescList, &varlogpb.ReplicaDescriptor{
				StorageNodeID: snid,
				Path:          "/tmp",
			})
		}

		metaDesc := &varlogpb.MetadataDescriptor{
			StorageNodes: sndescList,
			LogStreams: []*varlogpb.LogStreamDescriptor{
				{
					LogStreamID: logStreamID,
					Status:      varlogpb.LogStreamStatusRunning,
					Replicas:    replicaDescList,
				},
			},
		}

		Convey("When getting cluster metadata fails", func() {
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, verrors.ErrInternal).AnyTimes()

			Convey("Then Unseal should return error", func() {
				err := snManager.Unseal(context.TODO(), logStreamID)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When Unseal rpc to StorageNode fails", func() {
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(metaDesc, nil).AnyTimes()
			for i := 0; i < len(snmclList)-1; i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(nil)
			}
			snmclList[len(sndescList)-1].EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(verrors.ErrInternal)

			Convey("Then Unseal should fail", func() {
				err := snManager.Unseal(context.TODO(), logStreamID)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When Unseal rpc to StorageNode succeeds", func() {
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(metaDesc, nil).AnyTimes()
			for i := 0; i < len(snmclList); i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(nil)
			}

			Convey("Then Unseal should succeed", func() {
				err := snManager.Unseal(context.TODO(), logStreamID)
				So(err, ShouldBeNil)
			})
		})
	}))
}
