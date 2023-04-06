package admin

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func newTestReplicaSelector(t *testing.T, name string, cmview mrmanager.ClusterMetadataView, repfactor int) (ReplicaSelector, error) {
	switch strings.ToLower(name) {
	case "random":
		return newRandomReplicaSelector(cmview, repfactor)
	case "balanced":
		return newBalancedReplicaSelector(cmview, repfactor)
	default:
		return nil, fmt.Errorf("unknown selector: %s", name)
	}
}

func TestReplicaSelector_ReplicationFactorZero(t *testing.T) {
	tcs := []string{
		"Random",
		"Balanced",
	}

	for _, tc := range tcs {
		t.Run(tc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			cmview := mrmanager.NewMockClusterMetadataView(ctrl)
			cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil).AnyTimes()
			_, err := newTestReplicaSelector(t, tc, cmview, 0)
			require.Error(t, err)
		})
	}
}

func TestReplicaSelector_ClusterMetadataViewNil(t *testing.T) {
	tcs := []string{
		"Random",
		"Balanced",
	}

	for _, tc := range tcs {
		t.Run(tc, func(t *testing.T) {
			_, err := newTestReplicaSelector(t, tc, nil, 1)
			require.Error(t, err)
		})
	}
}

func TestReplicaSelector(t *testing.T) {
	type testCase struct {
		name      string
		md        *varlogpb.MetadataDescriptor
		repfactor int
	}

	getInfo := func(md *varlogpb.MetadataDescriptor) map[types.StorageNodeID]map[string]bool {
		snpaths := make(map[types.StorageNodeID]map[string]bool, len(md.StorageNodes))
		for _, snd := range md.StorageNodes {
			if _, ok := snpaths[snd.StorageNodeID]; !ok {
				snpaths[snd.StorageNodeID] = make(map[string]bool, len(snd.Paths))
			}
			for _, path := range snd.Paths {
				snpaths[snd.StorageNodeID][path] = true
			}
		}
		return snpaths
	}

	tcs := []*testCase{
		{
			name:      "Random",
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
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			cmview := mrmanager.NewMockClusterMetadataView(ctrl)
			cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(tc.md, nil).AnyTimes()

			s, err := newTestReplicaSelector(t, tc.name, cmview, tc.repfactor)
			require.NoError(t, err)

			require.Equal(t, strings.ToLower(tc.name), s.Name())

			replicas, err := s.Select(context.Background())
			require.NoError(t, err)

			snpaths := make(map[types.StorageNodeID]string, tc.repfactor)
			for _, replica := range replicas {
				require.False(t, replica.StorageNodeID.Invalid())
				require.NotEmpty(t, replica.StorageNodePath)
				snpaths[replica.StorageNodeID] = replica.StorageNodePath
			}
			require.Len(t, snpaths, tc.repfactor)

			mdSnpaths := getInfo(tc.md)
			for snid, snpath := range snpaths {
				require.Contains(t, mdSnpaths, snid)
				require.Contains(t, mdSnpaths[snid], snpath)
			}
		})
	}
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
					snd.Paths = make([]string, c.Intn(maxStoragesPerNode+1-minStoragesPerNode)+minStoragesPerNode)
					for k := 0; k < len(snd.Paths); k++ {
						snd.Paths[k] = fmt.Sprintf("/data%d", k+1)
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

		cmView := mrmanager.NewMockClusterMetadataView(ctrl)
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
		numPaths := len(md.StorageNodes[0].Paths)
		for _, snd := range md.StorageNodes[1:] {
			if numPaths != len(snd.Paths) {
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
		paths[storageNodeID] = len(snd.Paths)
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
	require.LessOrEqual(t, max/min, tolerance)
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
