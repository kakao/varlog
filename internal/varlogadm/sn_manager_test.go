package varlogadm

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/snc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/proto/vmspb"
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
			snmclList[0].EXPECT().AddLogStreamReplica(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(verrors.ErrInternal).MaxTimes(1)
			for i := 1; i < len(snmclList); i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().AddLogStreamReplica(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).MaxTimes(1)
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
				snmcl.EXPECT().AddLogStreamReplica(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
			topicID     = types.TopicID(1)
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
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snid,
					Address:       "127.0.0.1:" + strconv.Itoa(10000+int(snid)),
				},
				Status: varlogpb.StorageNodeStatusRunning,
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
				_, err := snManager.Seal(context.TODO(), topicID, logStreamID, types.MinGLSN)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When Seal rpc to StorageNode fails", func() {
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(metaDesc, nil).AnyTimes()
			const lastGLSN = types.GLSN(10)

			for i := 0; i < len(snmclList)-1; i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any(), lastGLSN).Return(varlogpb.LogStreamStatusSealed, lastGLSN, nil)
			}
			snmclList[len(sndescList)-1].EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any(), lastGLSN).Return(varlogpb.LogStreamStatusRunning, types.InvalidGLSN, verrors.ErrInternal)

			Convey("Then Seal should return response not having the failed node", func() {
				lsMetaDescList, err := snManager.Seal(context.TODO(), topicID, logStreamID, lastGLSN)
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
			topicID     = types.TopicID(1)
			logStreamID = types.LogStreamID(1)
		)

		var replicaDescList []*varlogpb.ReplicaDescriptor
		var sndescList []*varlogpb.StorageNodeDescriptor
		var snmclList []*snc.MockStorageNodeManagementClient
		for snid := types.StorageNodeID(1); snid <= nrSN; snid++ {
			peerAddr := "127.0.0.1:" + strconv.Itoa(10000+int(snid))
			snmcl := snc.NewMockStorageNodeManagementClient(ctrl)
			snmcl.EXPECT().PeerStorageNodeID().Return(snid).AnyTimes()
			snmcl.EXPECT().PeerAddress().Return(peerAddr).AnyTimes()
			snmcl.EXPECT().Close().Return(nil).AnyTimes()

			snManager.AddStorageNode(snmcl)

			snmclList = append(snmclList, snmcl)

			sndescList = append(sndescList, &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snid,
					Address:       peerAddr,
				},
				Status: varlogpb.StorageNodeStatusRunning,
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
				err := snManager.Unseal(context.TODO(), topicID, logStreamID)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When Unseal rpc to StorageNode fails", func() {
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(metaDesc, nil).AnyTimes()
			for i := 0; i < len(snmclList)-1; i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().Unseal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			}
			snmclList[len(sndescList)-1].EXPECT().Unseal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(verrors.ErrInternal).AnyTimes()

			Convey("Then Unseal should fail", func() {
				err := snManager.Unseal(context.TODO(), topicID, logStreamID)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When Unseal rpc to StorageNode succeeds", func() {
			cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(metaDesc, nil).AnyTimes()
			for i := 0; i < len(snmclList); i++ {
				snmcl := snmclList[i]
				snmcl.EXPECT().Unseal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			}

			Convey("Then Unseal should succeed", func() {
				err := snManager.Unseal(context.TODO(), topicID, logStreamID)
				So(err, ShouldBeNil)
			})
		})
	}))
}

func TestTrim(t *testing.T) {
	Convey("Given a StorageNodeManager", t, withTestStorageNodeManager(t, func(ctrl *gomock.Controller, snManager StorageNodeManager, cmView *MockClusterMetadataView) {
		// Replication Factor = 2, Number of Storage Nodes = 2
		// [TopicID = 1] LogStreamID = 1, 2, 3
		// [TopicID = 2] LogStreamID = 4, 5, 6
		const (
			snid1 = types.StorageNodeID(1)
			snid2 = types.StorageNodeID(2)

			tpid1 = types.TopicID(1)
			lsid1 = types.LogStreamID(1)
			lsid2 = types.LogStreamID(2)
			lsid3 = types.LogStreamID(3)

			tpid2 = types.TopicID(2)
			lsid4 = types.LogStreamID(4)
			lsid5 = types.LogStreamID(5)
			lsid6 = types.LogStreamID(6)
		)
		fakeError := errors.New("error")

		snmcl1 := snc.NewMockStorageNodeManagementClient(ctrl)
		snmcl1.EXPECT().Close().Return(nil).AnyTimes()
		snmcl1.EXPECT().PeerStorageNodeID().Return(snid1).AnyTimes()
		snmcl1.EXPECT().Trim(gomock.Any(), gomock.Eq(tpid1), gomock.Any()).Return(map[types.LogStreamID]error{
			lsid1: nil,
			lsid2: nil,
			lsid3: fakeError,
		}, nil).AnyTimes()
		snmcl1.EXPECT().Trim(gomock.Any(), gomock.Eq(tpid2), gomock.Any()).Return(map[types.LogStreamID]error{
			lsid4: nil,
			lsid5: nil,
			lsid6: nil,
		}, nil).AnyTimes()

		snmcl2 := snc.NewMockStorageNodeManagementClient(ctrl)
		snmcl2.EXPECT().Close().Return(nil).AnyTimes()
		snmcl2.EXPECT().PeerStorageNodeID().Return(snid2).AnyTimes()
		snmcl2.EXPECT().Trim(gomock.Any(), gomock.Eq(tpid1), gomock.Any()).Return(map[types.LogStreamID]error{
			lsid1: nil,
			lsid2: nil,
			lsid3: nil,
		}, nil).AnyTimes()
		snmcl2.EXPECT().Trim(gomock.Any(), gomock.Eq(tpid2), gomock.Any()).Return(nil, fakeError).AnyTimes()

		snManager.AddStorageNode(snmcl1)
		snManager.AddStorageNode(snmcl2)

		cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
			StorageNodes: []*varlogpb.StorageNodeDescriptor{},
			LogStreams: []*varlogpb.LogStreamDescriptor{
				{
					TopicID:     tpid1,
					LogStreamID: lsid1,
					Status:      varlogpb.LogStreamStatusRunning,
					Replicas: []*varlogpb.ReplicaDescriptor{
						{StorageNodeID: snid1},
						{StorageNodeID: snid2},
					},
				},
				{
					TopicID:     tpid1,
					LogStreamID: lsid2,
					Status:      varlogpb.LogStreamStatusRunning,
					Replicas: []*varlogpb.ReplicaDescriptor{
						{StorageNodeID: snid1},
						{StorageNodeID: snid2},
					},
				},
				{
					TopicID:     tpid1,
					LogStreamID: lsid3,
					Status:      varlogpb.LogStreamStatusRunning,
					Replicas: []*varlogpb.ReplicaDescriptor{
						{StorageNodeID: snid1},
						{StorageNodeID: snid2},
					},
				},
				{
					TopicID:     tpid2,
					LogStreamID: lsid4,
					Status:      varlogpb.LogStreamStatusRunning,
					Replicas: []*varlogpb.ReplicaDescriptor{
						{StorageNodeID: snid1},
						{StorageNodeID: snid2},
					},
				},
				{
					TopicID:     tpid2,
					LogStreamID: lsid5,
					Status:      varlogpb.LogStreamStatusRunning,
					Replicas: []*varlogpb.ReplicaDescriptor{
						{StorageNodeID: snid1},
						{StorageNodeID: snid2},
					},
				},
				{
					TopicID:     tpid2,
					LogStreamID: lsid6,
					Status:      varlogpb.LogStreamStatusRunning,
					Replicas: []*varlogpb.ReplicaDescriptor{
						{StorageNodeID: snid1},
						{StorageNodeID: snid2},
					},
				},
			},
			Topics: []*varlogpb.TopicDescriptor{
				{
					TopicID:    tpid1,
					Status:     varlogpb.TopicStatusRunning,
					LogStreams: []types.LogStreamID{lsid1, lsid2, lsid3},
				},
				{
					TopicID:    tpid2,
					Status:     varlogpb.TopicStatusRunning,
					LogStreams: []types.LogStreamID{lsid4, lsid5, lsid6},
				},
			},
		}, nil).AnyTimes()

		Convey("When some of the log stream replicas in a topic could not trim logs", func() {
			Convey("Then Trim should succeed", func() {
				result, err := snManager.Trim(context.Background(), tpid1, types.GLSN(10))
				So(err, ShouldBeNil)
				So(result, ShouldHaveLength, 6)
				So(result, ShouldContain, vmspb.TrimResult{
					StorageNodeID: snid1,
					LogStreamID:   lsid1,
				})
				So(result, ShouldContain, vmspb.TrimResult{
					StorageNodeID: snid2,
					LogStreamID:   lsid1,
				})
				So(result, ShouldContain, vmspb.TrimResult{
					StorageNodeID: snid1,
					LogStreamID:   lsid2,
				})
				So(result, ShouldContain, vmspb.TrimResult{
					StorageNodeID: snid2,
					LogStreamID:   lsid2,
				})
				So(result, ShouldContain, vmspb.TrimResult{
					StorageNodeID: snid1,
					LogStreamID:   lsid3,
					Error:         fakeError.Error(),
				})
				So(result, ShouldContain, vmspb.TrimResult{
					StorageNodeID: snid2,
					LogStreamID:   lsid3,
				})
			})
		})

		Convey("When some the Trim RPC fails", func() {
			Convey("Then Trim should fail", func() {
				_, err := snManager.Trim(context.Background(), tpid2, types.GLSN(10))
				So(err, ShouldNotBeNil)
			})
		})
	}))
}
