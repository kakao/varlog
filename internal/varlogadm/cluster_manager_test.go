package varlogadm

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/internal/varlogadm/mrmanager"
	"github.daumkakao.com/varlog/varlog/internal/varlogadm/snmanager"
	"github.daumkakao.com/varlog/varlog/internal/varlogadm/snwatcher"
	"github.daumkakao.com/varlog/varlog/internal/varlogadm/stats"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestAdmin_StorageNodes(t *testing.T) {
	const cid = types.ClusterID(1)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmview := mrmanager.NewMockClusterMetadataView(ctrl)
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		StorageNodes: []*varlogpb.StorageNodeDescriptor{
			{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: types.StorageNodeID(1),
				},
			},
			{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: types.StorageNodeID(2),
				},
			},
		},
	}, nil).AnyTimes()

	snmgr := snmanager.NewMockStorageNodeManager(ctrl)
	snmgr.EXPECT().GetMetadata(gomock.Any(), gomock.Eq(types.StorageNodeID(1))).Return(nil, errors.New("error")).AnyTimes()
	snmgr.EXPECT().GetMetadata(gomock.Any(), gomock.Eq(types.StorageNodeID(2))).Return(&snpb.StorageNodeMetadataDescriptor{
		ClusterID: cid,
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: types.StorageNodeID(2),
			Address:       "sn2",
		},
		Status: varlogpb.StorageNodeStatusRunning,
	}, nil).AnyTimes()

	mrmgr := mrmanager.NewMockMetadataRepositoryManager(ctrl)
	mrmgr.EXPECT().ClusterMetadataView().Return(cmview).AnyTimes()

	cm, err := NewClusterManager(context.TODO(), WithClusterID(cid), WithMetadataRepositoryManager(mrmgr), WithStorageNodeManager(snmgr))
	assert.NoError(t, err)

	snmds, err := cm.StorageNodes(context.Background())
	assert.NoError(t, err)
	assert.Len(t, snmds, 2)

	assert.Contains(t, snmds, types.StorageNodeID(1))
	assert.Equal(t, varlogpb.StorageNodeStatusUnavailable, snmds[types.StorageNodeID(1)].Status)

	assert.Contains(t, snmds, types.StorageNodeID(2))
	assert.Equal(t, varlogpb.StorageNodeStatusRunning, snmds[types.StorageNodeID(2)].Status)
}

func TestAdmin_DoNotSyncSealedReplicas(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		mu       sync.Mutex
		metadata = &varlogpb.MetadataDescriptor{
			StorageNodes: []*varlogpb.StorageNodeDescriptor{
				{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: types.StorageNodeID(1),
						Address:       "sn1",
					},
				},
				{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: types.StorageNodeID(2),
						Address:       "sn2",
					},
				},
			},
		}
	)

	cmview := mrmanager.NewMockClusterMetadataView(ctrl)
	mrmgr := mrmanager.NewMockMetadataRepositoryManager(ctrl)
	mrmgr.EXPECT().Close().Return(nil).AnyTimes()
	mrmgr.EXPECT().ClusterMetadataView().Return(cmview).AnyTimes()
	mrmgr.EXPECT().Seal(gomock.Any(), gomock.Any()).Return(types.GLSN(5), nil).AnyTimes()
	cmview.EXPECT().ClusterMetadata(gomock.Any()).DoAndReturn(func(context.Context) (*varlogpb.MetadataDescriptor, error) {
		mu.Lock()
		defer mu.Unlock()
		return proto.Clone(metadata).(*varlogpb.MetadataDescriptor), nil
	}).AnyTimes()

	var (
		lsrmd1 = snpb.LogStreamReplicaMetadataDescriptor{
			LogStreamReplica: varlogpb.LogStreamReplica{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: types.StorageNodeID(1),
				},
			},
			Status:              varlogpb.LogStreamStatusSealed,
			Version:             1,
			GlobalHighWatermark: 10,
		}
		lsrmd2 = snpb.LogStreamReplicaMetadataDescriptor{
			LogStreamReplica: varlogpb.LogStreamReplica{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: types.StorageNodeID(2),
				},
			},
			Status:              varlogpb.LogStreamStatusSealed,
			Version:             2,
			GlobalHighWatermark: 20,
		}
		getMetadataCounts = make(map[types.StorageNodeID]int64)
		lsstat            = stats.NewLogStreamStat(
			varlogpb.LogStreamStatusSealed,
			map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor{
				types.StorageNodeID(1): lsrmd1,
				types.StorageNodeID(2): lsrmd2,
			},
		)
		//lsstat = &statrepos.LogStreamStat{
		//	status: varlogpb.LogStreamStatusSealed,
		//	replicas: map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor{
		//		types.StorageNodeID(1): lsrmd1,
		//		types.StorageNodeID(2): lsrmd2,
		//	},
		//}
		statreposQueryCounts = 0
	)

	snmgr := snmanager.NewMockStorageNodeManager(ctrl)
	snmgr.EXPECT().Close().Return(nil).AnyTimes()
	snmgr.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, snid types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error) {
		mu.Lock()
		defer mu.Unlock()

		snd := metadata.GetStorageNode(snid)
		assert.NotNil(t, snd)

		defer func() {
			getMetadataCounts[snid]++
		}()

		assert.Contains(t, []types.StorageNodeID{1, 2}, snid)
		lsrmd, ok := lsstat.Replica(snid)
		assert.True(t, ok)
		//lsrmd := lsstat.replicas[snid]

		return &snpb.StorageNodeMetadataDescriptor{
			StorageNode:       snd.StorageNode,
			LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{lsrmd},
		}, nil
	}).AnyTimes()
	snmgr.EXPECT().Sync(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, types.TopicID, types.LogStreamID, types.StorageNodeID, types.StorageNodeID, types.GLSN) (*snpb.SyncStatus, error) {
			assert.Fail(t, "do not sync")
			return nil, errors.New("do not sync")
		},
	).AnyTimes()

	sp := stats.NewMockRepository(ctrl)
	sp.EXPECT().Report(gomock.Any(), gomock.Any()).Return().AnyTimes()
	sp.EXPECT().GetLogStream(gomock.Any()).DoAndReturn(
		func(types.LogStreamID) *stats.LogStreamStat {
			mu.Lock()
			defer mu.Unlock()
			statreposQueryCounts++
			return lsstat
		},
	).AnyTimes()

	mgr, err := NewClusterManager(context.TODO(),
		WithMetadataRepositoryManager(mrmgr),
		WithStorageNodeManager(snmgr),
		WithLogStreamGCTimeout(time.Duration(math.MaxInt64)),
		WithStorageNodeWatcherOptions(
			snwatcher.WithTick(10*time.Millisecond),
			snwatcher.WithReportInterval(10),
			snwatcher.WithHeartbeatTimeout(20),
		),
	)
	assert.NoError(t, err)
	mgr.statRepository = sp

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, mgr.Serve())
	}()
	defer func() {
		assert.NoError(t, mgr.Close())
		wg.Wait()
	}()

	// FIXME: Confirm that storage node watcher is running.
	time.Sleep(3 * time.Second)
	// assert.Eventually(t, func() bool {
	//	return snwatcher.(*snwatcher.snWatcher).runner.NumTasks() > 0
	// }, 3*time.Second, 10*time.Millisecond)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return getMetadataCounts[types.StorageNodeID(1)] > 0 && getMetadataCounts[types.StorageNodeID(2)] > 0
	}, time.Second, 10*time.Millisecond)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return statreposQueryCounts > 3
	}, time.Second, 10*time.Millisecond)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
