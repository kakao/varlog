package admin

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/golang/mock/gomock"
	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

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
