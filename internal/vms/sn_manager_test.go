package vms

import (
	"context"
	"strconv"
	"testing"

	gomock "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/proto/varlogpb"
	"go.uber.org/zap"
)

func withTestStorageNodeManager(t *testing.T, f func(ctrl *gomock.Controller, snManager StorageNodeManager, cmView *MockClusterMetadataView)) func() {
	return func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		meta := &varlogpb.MetadataDescriptor{}
		cmView := NewMockClusterMetadataView(ctrl)
		cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(meta, nil)

		snManager, err := NewStorageNodeManager(context.TODO(), cmView, zap.L())
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
			snmcl := varlog.NewMockManagementClient(ctrl)
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
			clusterID   = types.ClusterID(1)
			logStreamID = types.LogStreamID(1)
			nrSN        = 3
		)

		logStreamDesc := &varlogpb.LogStreamDescriptor{
			LogStreamID: logStreamID,
			Status:      varlogpb.LogStreamStatusRunning,
		}

		var snmclList []*varlog.MockManagementClient

		for snid := types.StorageNodeID(1); snid <= nrSN; snid++ {
			logStreamDesc.Replicas = append(logStreamDesc.Replicas, &varlogpb.ReplicaDescriptor{
				StorageNodeID: snid,
				Path:          "/tmp",
			})

			snmcl := varlog.NewMockManagementClient(ctrl)
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
				f := func() { snManager.AddLogStream(context.TODO(), logStreamDesc) }
				So(f, ShouldPanic)
			})
		})

		Convey("When LogStreamID already exists in metadata of storage node", func() {
			badSNMCL := snmclList[0]
			badSNMCL.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).Return(
				&varlogpb.StorageNodeMetadataDescriptor{
					ClusterID:   clusterID,
					StorageNode: &varlogpb.StorageNodeDescriptor{},
					LogStreams: []varlogpb.LogStreamMetadataDescriptor{
						{
							StorageNodeID: badSNMCL.PeerStorageNodeID(),
							LogStreamID:   logStreamID,
							Status:        varlogpb.LogStreamStatusRunning,
							HighWatermark: types.MinGLSN,
							Path:          "/tmp",
						},
					},
				}, nil).MaxTimes(1)

			for i := 0; i < len(snmclList); i++ {
				snmcl := snmclList[i]
				snManager.AddStorageNode(snmcl)
			}

			Convey("Then LogStream should not be added", func() {
				err := snManager.AddLogStream(context.TODO(), logStreamDesc)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When AddLogStream rpc to StorageNode fails", func() {
			for i := 0; i < len(snmclList); i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).Return(
					&varlogpb.StorageNodeMetadataDescriptor{
						ClusterID:   clusterID,
						StorageNode: &varlogpb.StorageNodeDescriptor{},
						LogStreams:  nil,
					}, nil,
				)

				snManager.AddStorageNode(snmcl)

				var err error
				if i == 1 {
					err = varlog.ErrInternal
				}
				snmcl.EXPECT().AddLogStream(gomock.Any(), gomock.Any(), gomock.Any()).Return(err).MaxTimes(1)
			}

			Convey("Then LogStream should not be added", func() {
				err := snManager.AddLogStream(context.TODO(), logStreamDesc)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When AddLogStream rpc to StorageNode succeeds", func() {
			for i := 0; i < len(snmclList); i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).Return(
					&varlogpb.StorageNodeMetadataDescriptor{
						ClusterID:   clusterID,
						StorageNode: &varlogpb.StorageNodeDescriptor{},
						LogStreams:  nil,
					}, nil,
				)

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
		var snmclList []*varlog.MockManagementClient
		for snid := types.StorageNodeID(1); snid <= nrSN; snid++ {
			snmcl := varlog.NewMockManagementClient(ctrl)
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
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, varlog.ErrInternal)
			Convey("Then Seal shoud return error", func() {
				_, err := snManager.Seal(context.TODO(), logStreamID, types.MinGLSN)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When Seal rpc to StorageNode fails", func() {
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(metaDesc, nil)
			const lastGLSN = types.GLSN(10)

			for i := 0; i < len(snmclList)-1; i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().Seal(gomock.Any(), gomock.Any(), lastGLSN).Return(varlogpb.LogStreamStatusSealed, lastGLSN, nil)
			}
			snmclList[len(sndescList)-1].EXPECT().Seal(gomock.Any(), gomock.Any(), lastGLSN).Return(varlogpb.LogStreamStatusRunning, types.InvalidGLSN, varlog.ErrInternal)

			Convey("Then Seal should return response not having the failed node", func() {
				lsMetaDescList, err := snManager.Seal(context.TODO(), logStreamID, lastGLSN)
				So(err, ShouldBeNil)
				So(len(lsMetaDescList), ShouldEqual, nrSN-1)

				var snIDs []types.StorageNodeID
				for _, lsMeta := range lsMetaDescList {
					snIDs = append(snIDs, lsMeta.GetStorageNodeID())
				}

				So(snIDs, ShouldContain, types.StorageNodeID(1))
				So(snIDs, ShouldContain, types.StorageNodeID(2))
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
		var snmclList []*varlog.MockManagementClient
		for snid := types.StorageNodeID(1); snid <= nrSN; snid++ {
			snmcl := varlog.NewMockManagementClient(ctrl)
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
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, varlog.ErrInternal)

			Convey("Then Unseal should return error", func() {
				err := snManager.Unseal(context.TODO(), logStreamID)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When Unseal rpc to StorageNode fails", func() {
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(metaDesc, nil)
			for i := 0; i < len(snmclList)-1; i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(nil)
			}
			snmclList[len(sndescList)-1].EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(varlog.ErrInternal)

			Convey("Then Unseal should fail", func() {
				err := snManager.Unseal(context.TODO(), logStreamID)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When Unseal rpc to StorageNode succeeds", func() {
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(metaDesc, nil)
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
