package varlogadm

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/golang/mock/gomock"
	fuzz "github.com/google/gofuzz"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: types.StorageNodeID(1),
						},
						Storages: []*varlogpb.StorageDescriptor{
							{
								Path: "/tmp",
							},
						},
					},
					{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: types.StorageNodeID(2),
						},
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
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: snid,
							},
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
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: snid,
							},
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
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: snid,
							},
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

func TestBalancedReplicaSelector(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		testCount          = 100
		replicationFactor  = 3
		maxStorageNodes    = 10
		minStoragesPerNode = 1
		maxStoragesPerNode = 4
		numLogStreams      = maxStorageNodes * maxStoragesPerNode / replicationFactor
	)

	for i := 0; i < testCount; i++ {
		f := fuzz.New().NilChance(0).Funcs(
			func(snds *[]*varlogpb.StorageNodeDescriptor, c fuzz.Continue) {
				cnt := c.Intn(maxStorageNodes+1-replicationFactor) + replicationFactor
				*snds = make([]*varlogpb.StorageNodeDescriptor, cnt)
				for j := 0; j < cnt; j++ {
					snd := &varlogpb.StorageNodeDescriptor{}
					snd.StorageNodeID = types.StorageNodeID(j + 1)
					snd.Storages = make([]*varlogpb.StorageDescriptor, c.Intn(maxStoragesPerNode+1-minStoragesPerNode)+minStoragesPerNode)
					for k := 0; k < len(snd.Storages); k++ {
						snd.Storages[k] = &varlogpb.StorageDescriptor{
							Path: fmt.Sprintf("/data%d", k+1),
						}
					}
					(*snds)[j] = snd
				}
			},
			func(lsds *[]*varlogpb.LogStreamDescriptor, c fuzz.Continue) {
				*lsds = nil
			},
		)
		md := &varlogpb.MetadataDescriptor{}
		f.Fuzz(md)

		cmView := NewMockClusterMetadataView(ctrl)
		cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(md, nil).AnyTimes()

		sel, err := newBalancedReplicaSelector(cmView, replicationFactor)
		require.NoError(t, err)

		for j := 0; j < numLogStreams; j++ {
			rds, err := sel.Select(context.Background())
			require.NoError(t, err)
			require.Len(t, rds, replicationFactor)

			err = md.InsertLogStream(&varlogpb.LogStreamDescriptor{
				LogStreamID: types.LogStreamID(j + 1),
				Replicas:    rds,
			})
			require.NoError(t, err)
		}

		// Very generous tolerance
		tolerance := float64(numLogStreams * replicationFactor / len(md.StorageNodes))
		equalNumStorages := true
		numStorages := len(md.StorageNodes[0].Storages)
		for _, snd := range md.StorageNodes[1:] {
			if numStorages != len(snd.Storages) {
				equalNumStorages = false
				break
			}
		}
		if equalNumStorages {
			tolerance = 1.3
		}
		testVerifyLogStreamDescriptors(t, md, tolerance)
	}
}

func testVerifyLogStreamDescriptors(t *testing.T, md *varlogpb.MetadataDescriptor, tolerance float64) {
	paths := make(map[types.StorageNodeID]int)
	replicas := make(map[types.StorageNodeID]int)
	primaries := make(map[types.StorageNodeID]int)
	for _, lsd := range md.LogStreams {
		for i, rd := range lsd.Replicas {
			storageNodeID := rd.StorageNodeID
			replicas[storageNodeID]++
			if i == 0 {
				primaries[storageNodeID]++
			}
		}
	}
	for _, snd := range md.StorageNodes {
		storageNodeID := snd.StorageNodeID
		paths[storageNodeID] = len(snd.Storages)
	}

	testUtilizationBalance(t, paths, replicas, tolerance)
	testPrimariesBalance(t, primaries, tolerance)
}

func testUtilizationBalance(t *testing.T, paths, replicas map[types.StorageNodeID]int, tolerance float64) {
	us := make([]float64, 0, len(paths))
	for snid, p := range paths {
		r, ok := replicas[snid]
		if !ok {
			us = append(us, 0)
			continue
		}
		us = append(us, float64(r)/float64(p))
	}
	sort.Float64s(us)
	min, max := us[0], us[len(us)-1]
	if !assert.LessOrEqual(t, max/min, tolerance) {
		t.Logf("paths: %+v", paths)
		t.Logf("replicas: %+v", replicas)
	}
}

func testPrimariesBalance(t *testing.T, primaries map[types.StorageNodeID]int, tolerance float64) {
	ps := make([]int, 0, len(primaries))
	for _, p := range primaries {
		ps = append(ps, p)
	}
	sort.Ints(ps)
	min, max := ps[0], ps[len(ps)-1]
	if !assert.LessOrEqual(t, float64(max-min), tolerance) {
		t.Logf("primaries: %+v", primaries)
	}
}
