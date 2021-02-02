package vms

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestRandomSelector(t *testing.T) {
	Convey("Given RandomSelector", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		cmView := NewMockClusterMetadataView(ctrl)
		cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(
			&varlogpb.MetadataDescriptor{
				StorageNodes: []*varlogpb.StorageNodeDescriptor{
					{
						StorageNodeID: types.StorageNodeID(1),
						Storages: []*varlogpb.StorageDescriptor{
							{
								Path: "/tmp",
							},
						},
					},
					{
						StorageNodeID: types.StorageNodeID(2),
						Storages: []*varlogpb.StorageDescriptor{
							{
								Path: "/tmp",
							},
						},
					},
				},
				LogStreams: []*varlogpb.LogStreamDescriptor{
					{
						LogStreamID: types.LogStreamID(1),
						Status:      varlogpb.LogStreamStatusRunning,
						Replicas: []*varlogpb.ReplicaDescriptor{
							{
								StorageNodeID: types.StorageNodeID(1),
								Path:          "/tmp",
							},
						},
					},
				},
			},
			nil,
		).MaxTimes(1)

		Convey("Invalid argument in constructor", func() {
			_, err := newRandomReplicaSelector(cmView, 0)
			So(err, ShouldNotBeNil)
		})

		Convey("When there are not enough replicas to select", func() {
			Convey("Then Select should return an error", func() {
				selector, err := newRandomReplicaSelector(cmView, 2, types.StorageNodeID(1))
				So(err, ShouldBeNil)

				_, err = selector.Select(context.TODO())
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When there are enough replicas to select", func() {
			Convey("Then Select should return replicas", func() {
				selector, err := newRandomReplicaSelector(cmView, 1, types.StorageNodeID(1))
				So(err, ShouldBeNil)

				replicas, err := selector.Select(context.TODO())
				So(err, ShouldBeNil)
				So(replicas[0].GetStorageNodeID(), ShouldEqual, types.StorageNodeID(2))
			})

			Convey("Then Select should return replicas which are most idle", func() {
				selector, err := newRandomReplicaSelector(cmView, 1)
				So(err, ShouldBeNil)

				replicas, err := selector.Select(context.TODO())
				So(err, ShouldBeNil)
				So(replicas[0].GetStorageNodeID(), ShouldEqual, types.StorageNodeID(2))
			})
		})

	})
}

func TestVictimSelector(t *testing.T) {
	Convey("Given VictimSelector", t, func() {
		const logStreamID = types.LogStreamID(1)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		snMgr := NewMockStorageNodeManager(ctrl)
		replicas := []*varlogpb.ReplicaDescriptor{
			{
				StorageNodeID: types.StorageNodeID(1),
				Path:          "/tmp",
			},
			{
				StorageNodeID: types.StorageNodeID(2),
				Path:          "/tmp",
			},
		}

		Convey("When all replicas are LogStreamStatusSealed, thus none are victims", func() {
			snMgr.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, snid types.StorageNodeID) (*varlogpb.StorageNodeMetadataDescriptor, error) {
					return &varlogpb.StorageNodeMetadataDescriptor{
						StorageNode: &varlogpb.StorageNodeDescriptor{
							StorageNodeID: snid,
						},
						LogStreams: []varlogpb.LogStreamMetadataDescriptor{
							{
								StorageNodeID: snid,
								LogStreamID:   logStreamID,
								Status:        varlogpb.LogStreamStatusSealed,
							},
						},
					}, nil

				},
			).AnyTimes()

			Convey("Then Select should return an error", func() {
				selector := newVictimSelector(snMgr, logStreamID, replicas)
				_, err := selector.Select(context.TODO())
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "victimselector: no victim")
			})

		})

		Convey("When all replicas are not LogStreamStatusSealed, thus all are victims", func() {
			snMgr.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, snid types.StorageNodeID) (*varlogpb.StorageNodeMetadataDescriptor, error) {
					return &varlogpb.StorageNodeMetadataDescriptor{
						StorageNode: &varlogpb.StorageNodeDescriptor{
							StorageNodeID: snid,
						},
						LogStreams: []varlogpb.LogStreamMetadataDescriptor{
							{
								StorageNodeID: snid,
								LogStreamID:   logStreamID,
								Status:        varlogpb.LogStreamStatusSealing,
							},
						},
					}, nil

				},
			).AnyTimes()

			Convey("Then Select should return an error", func() {
				selector := newVictimSelector(snMgr, logStreamID, replicas)
				_, err := selector.Select(context.TODO())
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "victimselector: no good replica")
			})
		})

		Convey("When there are LogStreamStatusSealed and non-LogStreamStatusSealed", func() {
			snMgr.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, snid types.StorageNodeID) (*varlogpb.StorageNodeMetadataDescriptor, error) {
					var status varlogpb.LogStreamStatus
					switch snid {
					case types.StorageNodeID(1):
						status = varlogpb.LogStreamStatusSealed
					default:
						status = varlogpb.LogStreamStatusSealing
					}
					return &varlogpb.StorageNodeMetadataDescriptor{
						StorageNode: &varlogpb.StorageNodeDescriptor{
							StorageNodeID: snid,
						},
						LogStreams: []varlogpb.LogStreamMetadataDescriptor{
							{
								StorageNodeID: snid,
								LogStreamID:   logStreamID,
								Status:        status,
							},
						},
					}, nil

				},
			).AnyTimes()

			Convey("Then Select should return victims", func() {
				selector := newVictimSelector(snMgr, logStreamID, replicas)
				victims, err := selector.Select(context.TODO())
				So(err, ShouldBeNil)
				So(victims[0].GetStorageNodeID(), ShouldEqual, types.StorageNodeID(2))
			})
		})
	})
}
