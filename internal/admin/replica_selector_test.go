package admin

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/maps"

	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestReplicaSelector_NewUnknown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmview := mrmanager.NewMockClusterMetadataView(ctrl)
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil).AnyTimes()

	_, err := NewReplicaSelector("foo", cmview, 1)
	require.Error(t, err)

}

func TestReplicaSelector_New(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmview := mrmanager.NewMockClusterMetadataView(ctrl)
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil).AnyTimes()

	selectors := []string{
		ReplicaSelectorNameRandom,
		ReplicaSelectorNameLFU,
	}
	tcs := []struct {
		name      string
		cmview    mrmanager.ClusterMetadataView
		repfactor int
	}{
		{
			name:      "ReplicationFactorZero",
			cmview:    cmview,
			repfactor: 0,
		},
		{
			name:      "ClusterMetadataViewNil",
			cmview:    nil,
			repfactor: 1,
		},
	}

	for _, tc := range tcs {
		for _, selector := range selectors {
			t.Run(selector+tc.name, func(t *testing.T) {
				_, err := NewReplicaSelector(selector, tc.cmview, tc.repfactor)
				require.Error(t, err)
			})
		}
	}
}

func testCheckInvariant(t *testing.T, md *varlogpb.MetadataDescriptor, repfactor int, selectedReplicas []*varlogpb.ReplicaDescriptor) {
	selectedStorageNodePaths := make(map[types.StorageNodeID]string, repfactor)
	for _, replica := range selectedReplicas {
		require.False(t, replica.StorageNodeID.Invalid())
		require.NotEmpty(t, replica.StorageNodePath)
		selectedStorageNodePaths[replica.StorageNodeID] = replica.StorageNodePath
	}
	require.Len(t, selectedStorageNodePaths, repfactor)

	allStorageNodePaths := make(map[types.StorageNodeID]map[string]bool, len(md.StorageNodes))
	for _, snd := range md.StorageNodes {
		if _, ok := allStorageNodePaths[snd.StorageNodeID]; !ok {
			allStorageNodePaths[snd.StorageNodeID] = make(map[string]bool, len(snd.Paths))
		}
		for _, path := range snd.Paths {
			allStorageNodePaths[snd.StorageNodeID][path] = true
		}
	}
	for snid, snpath := range selectedStorageNodePaths {
		require.Contains(t, allStorageNodePaths, snid)
		require.Contains(t, allStorageNodePaths[snid], snpath)
	}
}

func TestReplicaSelector(t *testing.T) {
	const tpid = types.TopicID(1)

	type testCase struct {
		name      string
		selectors []string
		md        *varlogpb.MetadataDescriptor
		repfactor int
		testf     func(t *testing.T, tc *testCase, s ReplicaSelector)
	}

	tcs := []*testCase{
		{
			name:      "NotEnoughStorageNodes",
			selectors: []string{ReplicaSelectorNameRandom, ReplicaSelectorNameLFU},
			repfactor: 1,
			md:        &varlogpb.MetadataDescriptor{},
			testf: func(t *testing.T, _ *testCase, s ReplicaSelector) {
				_, err := s.Select(context.Background())
				require.Error(t, err)
			},
		},
		{
			name:      "InvalidClusterMetadataInvalidStorageNodeID",
			selectors: []string{ReplicaSelectorNameRandom, ReplicaSelectorNameLFU},
			repfactor: 1,
			md: &varlogpb.MetadataDescriptor{
				StorageNodes: []*varlogpb.StorageNodeDescriptor{
					{
						StorageNode: varlogpb.StorageNode{Address: "sn1"},
						Paths:       []string{},
					},
				},
				LogStreams: []*varlogpb.LogStreamDescriptor{},
			},
			testf: func(t *testing.T, _ *testCase, s ReplicaSelector) {
				_, err := s.Select(context.Background())
				require.Error(t, err)
			},
		},
		{
			name:      "InvalidClusterMetadataNoStoragePath",
			selectors: []string{ReplicaSelectorNameRandom, ReplicaSelectorNameLFU},
			repfactor: 1,
			md: &varlogpb.MetadataDescriptor{
				StorageNodes: []*varlogpb.StorageNodeDescriptor{
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 1, Address: "sn1"},
						Paths:       []string{},
					},
				},
				LogStreams: []*varlogpb.LogStreamDescriptor{},
			},
			testf: func(t *testing.T, _ *testCase, s ReplicaSelector) {
				_, err := s.Select(context.Background())
				require.Error(t, err)
			},
		},
		{
			name:      "InvalidClusterMetadataNoStorageNode",
			selectors: []string{ReplicaSelectorNameLFU},
			repfactor: 1,
			md: &varlogpb.MetadataDescriptor{
				StorageNodes: []*varlogpb.StorageNodeDescriptor{
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 1, Address: "sn1"},
						Paths:       []string{"/data1"},
					},
				},
				LogStreams: []*varlogpb.LogStreamDescriptor{
					{
						LogStreamID: 1,
						Replicas: []*varlogpb.ReplicaDescriptor{
							{
								StorageNodeID:   2,
								StorageNodePath: "/data1",
								DataPath:        "/data1/foo",
							},
						},
					},
				},
			},
			testf: func(t *testing.T, _ *testCase, s ReplicaSelector) {
				_, err := s.Select(context.Background())
				require.Error(t, err)
			},
		},
		{
			name:      "InvalidClusterMetadataNoStorageNodePath",
			selectors: []string{ReplicaSelectorNameLFU},
			repfactor: 1,
			md: &varlogpb.MetadataDescriptor{
				StorageNodes: []*varlogpb.StorageNodeDescriptor{
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 1, Address: "sn1"},
						Paths:       []string{"/data2"},
					},
				},
				LogStreams: []*varlogpb.LogStreamDescriptor{
					{
						LogStreamID: 1,
						Replicas: []*varlogpb.ReplicaDescriptor{
							{
								StorageNodeID:   1,
								StorageNodePath: "/data1",
								DataPath:        "/data1/foo",
							},
						},
					},
				},
			},
			testf: func(t *testing.T, _ *testCase, s ReplicaSelector) {
				_, err := s.Select(context.Background())
				require.Error(t, err)
			},
		},
		{
			name:      "Select",
			selectors: []string{ReplicaSelectorNameRandom},
			repfactor: 2,
			md: &varlogpb.MetadataDescriptor{
				StorageNodes: []*varlogpb.StorageNodeDescriptor{
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 1, Address: "sn1"},
						Paths:       []string{"/data1", "/data2"},
					},
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 2, Address: "sn2"},
						Paths:       []string{"/data1", "/data2"},
					},
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 3, Address: "sn3"},
						Paths:       []string{"/data1", "/data2"},
					},
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 4, Address: "sn4"},
						Paths:       []string{"/data1", "/data2"},
					},
				},
			},
			testf: func(t *testing.T, tc *testCase, s ReplicaSelector) {
				for i := 0; i < 10; i++ {
					replicas, err := s.Select(context.Background())
					require.NoError(t, err)

					testCheckInvariant(t, tc.md, tc.repfactor, replicas)

					err = tc.md.InsertLogStream(&varlogpb.LogStreamDescriptor{
						TopicID:     tpid,
						LogStreamID: types.LogStreamID(i + 1),
						Replicas:    replicas,
					})
					require.NoError(t, err)
				}
			},
		},
		{
			name:      "Select",
			selectors: []string{ReplicaSelectorNameLFU},
			repfactor: 3,
			md: &varlogpb.MetadataDescriptor{
				StorageNodes: []*varlogpb.StorageNodeDescriptor{
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 1, Address: "sn1"},
						Paths:       []string{"/data1", "/data2", "/data3", "/data4"},
					},
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 2, Address: "sn2"},
						Paths:       []string{"/data1", "/data2", "/data3", "/data4"},
					},
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 3, Address: "sn3"},
						Paths:       []string{"/data1", "/data2", "/data3", "/data4"},
					},
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 4, Address: "sn4"},
						Paths:       []string{"/data1", "/data2", "/data3", "/data4"},
					},
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 5, Address: "sn5"},
						Paths:       []string{"/data1", "/data2", "/data3", "/data4"},
					},
					{
						StorageNode: varlogpb.StorageNode{StorageNodeID: 6, Address: "sn6"},
						Paths:       []string{"/data1", "/data2", "/data3", "/data4"},
					},
				},
				LogStreams: []*varlogpb.LogStreamDescriptor{},
			},
			testf: func(t *testing.T, tc *testCase, s ReplicaSelector) {
				for i := 0; i < 100; i++ {
					replicas, err := s.Select(context.Background())
					require.NoError(t, err)

					testCheckInvariant(t, tc.md, tc.repfactor, replicas)

					err = tc.md.InsertLogStream(&varlogpb.LogStreamDescriptor{
						TopicID:     tpid,
						LogStreamID: types.LogStreamID(i + 1),
						Replicas:    replicas,
					})
					require.NoError(t, err)

					replicasCounts := make(map[types.StorageNodeID]int)
					primariesCounts := make(map[types.StorageNodeID]int)

					for _, lsd := range tc.md.LogStreams {
						for i, rd := range lsd.Replicas {
							snid := rd.StorageNodeID
							replicasCounts[snid]++
							if i == 0 {
								primariesCounts[snid]++
							}
						}
					}

					counts := maps.Values(replicasCounts)
					sort.Ints(counts)
					diff := counts[len(counts)-1] - counts[0]
					require.LessOrEqual(t, diff, 1)

					counts = maps.Values(primariesCounts)
					sort.Ints(counts)
					diff = counts[len(counts)-1] - counts[0]
					require.LessOrEqual(t, diff, 2)
				}
			},
		},
	}

	for _, tc := range tcs {
		for _, selector := range tc.selectors {
			t.Run(selector+tc.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				cmview := mrmanager.NewMockClusterMetadataView(ctrl)
				cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(tc.md, nil).AnyTimes()

				s, err := NewReplicaSelector(selector, cmview, tc.repfactor)
				require.NoError(t, err)
				require.Equal(t, selector, s.Name())
				tc.testf(t, tc, s)
			})
		}
	}
}
