package varlogadm

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"

	"github.com/kakao/varlog/internal/varlogadm/mrmanager"
	"github.com/kakao/varlog/internal/varlogadm/snmanager"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

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

	var cm = &ClusterManager{}
	cm.snMgr = snmgr
	cm.mrMgr = mrmgr
	cm.clusterID = cid
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
		lsstat            = &LogStreamStat{
			status: varlogpb.LogStreamStatusSealed,
			replicas: map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor{
				types.StorageNodeID(1): lsrmd1,
				types.StorageNodeID(2): lsrmd2,
			},
		}
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
		lsrmd := lsstat.replicas[snid]

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

	statrepos := NewMockStatRepository(ctrl)
	statrepos.EXPECT().Report(gomock.Any(), gomock.Any()).Return().AnyTimes()
	statrepos.EXPECT().GetLogStream(gomock.Any()).DoAndReturn(
		func(types.LogStreamID) *LogStreamStat {
			mu.Lock()
			defer mu.Unlock()
			statreposQueryCounts++
			return lsstat
		},
	).AnyTimes()

	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	mgr := &ClusterManager{
		statRepository: statrepos,
		server:         grpcServer,
		healthServer:   health.NewServer(),
	}
	mgr.mrMgr = mrmgr
	mgr.snMgr = snmgr
	mgr.listenAddress = "127.0.0.1:0"
	mgr.logger = zap.NewNop()

	snwatcherOpts := []WatcherOption{
		WithWatcherTick(10 * time.Millisecond),
		WithWatcherReportInterval(10),
		WithWatcherHeartbeatTimeout(20),
		WithWatcherGCTimeout(time.Duration(math.MaxInt64)),
	}

	snwatcher := NewStorageNodeWatcher(snwatcherOpts, cmview, snmgr, mgr, zap.NewNop())
	mgr.snWatcher = snwatcher

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

	assert.Eventually(t, func() bool {
		return snwatcher.(*snWatcher).runner.NumTasks() > 0
	}, 3*time.Second, 10*time.Millisecond)

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
