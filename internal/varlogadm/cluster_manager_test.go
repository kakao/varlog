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
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner/stopwaiter"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestAdmin_StorageNodes(t *testing.T) {
	const cid = types.ClusterID(1)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmview := NewMockClusterMetadataView(ctrl)
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

	snmgr := NewMockStorageNodeManager(ctrl)
	snmgr.EXPECT().GetMetadata(gomock.Any(), gomock.Eq(types.StorageNodeID(1))).Return(nil, errors.New("error")).AnyTimes()
	snmgr.EXPECT().GetMetadata(gomock.Any(), gomock.Eq(types.StorageNodeID(2))).Return(&snpb.StorageNodeMetadataDescriptor{
		ClusterID: cid,
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(2),
				Address:       "sn2",
			},
			Status: varlogpb.StorageNodeStatusRunning,
		},
	}, nil).AnyTimes()

	var cm ClusterManager = &clusterManager{
		cmView: cmview,
		snMgr:  snmgr,
		options: &Options{
			ClusterID: cid,
		},
	}
	snmds, err := cm.StorageNodes(context.Background())
	assert.NoError(t, err)
	assert.Len(t, snmds, 2)

	assert.Contains(t, snmds, types.StorageNodeID(1))
	assert.Equal(t, varlogpb.StorageNodeStatusUnavailable, snmds[types.StorageNodeID(1)].StorageNode.Status)

	assert.Contains(t, snmds, types.StorageNodeID(2))
	assert.Equal(t, varlogpb.StorageNodeStatusRunning, snmds[types.StorageNodeID(2)].StorageNode.Status)
}

func TestAdmin_DoNotSyncSealedReplicas(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	snwatcherOpts := WatcherOptions{
		Tick:             10 * time.Millisecond,
		ReportInterval:   10,
		HeartbeatTimeout: 20,
		GCTimeout:        time.Duration(math.MaxInt64),
	}

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

	cmview := NewMockClusterMetadataView(ctrl)
	mrmgr := NewMockMetadataRepositoryManager(ctrl)
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

	snmgr := NewMockStorageNodeManager(ctrl)
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
			StorageNode:       proto.Clone(snd).(*varlogpb.StorageNodeDescriptor),
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

	mgr := &clusterManager{
		cmState:        clusterManagerReady,
		mrMgr:          mrmgr,
		snMgr:          snmgr,
		cmView:         cmview,
		statRepository: statrepos,
		options: &Options{
			ListenAddress:  "127.0.0.1:0",
			WatcherOptions: snwatcherOpts,
		},
		server:       grpcServer,
		healthServer: health.NewServer(),
		sw:           stopwaiter.New(),
		logger:       zap.NewNop(),
	}
	snwatcher := NewStorageNodeWatcher(snwatcherOpts, cmview, snmgr, mgr, zap.NewNop())
	mgr.snWatcher = snwatcher
	assert.NoError(t, mgr.Run())

	defer func() {
		assert.NoError(t, mgr.Close())
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
