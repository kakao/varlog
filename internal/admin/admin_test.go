package admin_test

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/kakao/varlog/internal/admin"
	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/internal/admin/snmanager"
	"github.com/kakao/varlog/internal/admin/snwatcher"
	"github.com/kakao/varlog/internal/admin/stats"
	"github.com/kakao/varlog/internal/storagenode/volume"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/admpb"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestAdmin_InvalidConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mrmgr := mrmanager.NewMockMetadataRepositoryManager(ctrl)
	snmgr := snmanager.NewMockStorageNodeManager(ctrl)
	cmview := mrmanager.NewMockClusterMetadataView(ctrl)
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil).AnyTimes()
	mrmgr.EXPECT().ClusterMetadataView().Return(cmview).AnyTimes()

	// no listen address
	_, err := admin.New(ctx,
		admin.WithListenAddress(""),
		admin.WithMetadataRepositoryManager(mrmgr),
		admin.WithStorageNodeManager(snmgr),
	)
	assert.Error(t, err)

	// invalid replication factor
	_, err = admin.New(ctx,
		admin.WithReplicationFactor(0),
		admin.WithMetadataRepositoryManager(mrmgr),
		admin.WithStorageNodeManager(snmgr),
	)
	assert.Error(t, err)

	// no mr manager
	_, err = admin.New(ctx, admin.WithStorageNodeManager(snmgr))
	assert.Error(t, err)

	// no sn manager
	_, err = admin.New(ctx, admin.WithMetadataRepositoryManager(mrmgr))
	assert.Error(t, err)

	// nil logger
	_, err = admin.New(ctx,
		admin.WithMetadataRepositoryManager(mrmgr),
		admin.WithStorageNodeManager(snmgr),
		admin.WithLogger(nil),
	)
	assert.Error(t, err)
}

func TestAdminConstructor_UnfetchableClusterMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mrmgr := mrmanager.NewMockMetadataRepositoryManager(ctrl)
	cmview := mrmanager.NewMockClusterMetadataView(ctrl)
	snmgr := snmanager.NewMockStorageNodeManager(ctrl)

	mrmgr.EXPECT().ClusterMetadataView().Return(cmview).AnyTimes()
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, errors.New("error")).AnyTimes()

	_, err := admin.New(context.Background(),
		admin.WithMetadataRepositoryManager(mrmgr),
		admin.WithStorageNodeManager(snmgr),
	)
	assert.Error(t, err)
}

type testMock struct {
	*mrmanager.MockMetadataRepositoryManager
	*mrmanager.MockClusterMetadataView
	*snmanager.MockStorageNodeManager
	*admin.MockReplicaSelector
	*stats.MockRepository
}

func newTestMock(ctrl *gomock.Controller) *testMock {
	tm := &testMock{
		MockMetadataRepositoryManager: mrmanager.NewMockMetadataRepositoryManager(ctrl),
		MockClusterMetadataView:       mrmanager.NewMockClusterMetadataView(ctrl),
		MockStorageNodeManager:        snmanager.NewMockStorageNodeManager(ctrl),
		MockReplicaSelector:           admin.NewMockReplicaSelector(ctrl),
		MockRepository:                stats.NewMockRepository(ctrl),
	}

	tm.MockMetadataRepositoryManager.EXPECT().ClusterMetadataView().Return(tm.MockClusterMetadataView).AnyTimes()
	tm.MockMetadataRepositoryManager.EXPECT().Close().Return(nil).AnyTimes()
	tm.MockStorageNodeManager.EXPECT().Close().Return(nil).AnyTimes()

	return tm
}

func newTestClient(t *testing.T, addr string) (varlog.Admin, func()) {
	t.Helper()
	client, err := varlog.NewAdmin(context.Background(), addr)
	assert.NoError(t, err)
	closer := func() {
		assert.NoError(t, client.Close())
	}
	return client, closer
}

func TestAdmin_GetStorageNode(t *testing.T) {
	const (
		snid = types.StorageNodeID(1)
	)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "NoSuchStorageNode",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						StorageNodes: []*varlogpb.StorageNodeDescriptor{
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid,
								},
							},
						},
					}, nil,
				).AnyTimes()
				mock.MockRepository.EXPECT().GetStorageNode(snid).Return(&admpb.StorageNodeMetadata{
					StorageNodeMetadataDescriptor: snpb.StorageNodeMetadataDescriptor{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid,
						},
					},
				}, true)
			},
		},
		{
			name:    "Success without exact status",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						StorageNodes: []*varlogpb.StorageNodeDescriptor{
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid,
								},
							},
						},
					}, nil,
				).AnyTimes()
				mock.MockRepository.EXPECT().GetStorageNode(snid).Return(nil, false)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
					snwatcher.WithStatisticsRepository(mock.MockRepository),
				),
				admin.WithStatisticsRepository(mock.MockRepository),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.GetStorageNode(context.Background(), snid)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// This test verifies whether the expected logstreams are sealed
// in the event of an RPC timeout due to a storagenode failure.
// In addition, it verifies that the logstreams on the healthy node
// is not sealed in the event of intermittent failure of the mr request in that situation.
func TestAdmin_Storagenode_HeartbeatTimeout_Seal(t *testing.T) {
	const (
		cid         = types.ClusterID(1)
		tpid        = types.TopicID(1)
		victimsnid  = types.StorageNodeID(1)
		normalsnid  = types.StorageNodeID(2)
		victimlsid1 = types.LogStreamID(1)
		victimlsid2 = types.LogStreamID(2)
		normallsid1 = types.LogStreamID(3)
		normallsid2 = types.LogStreamID(4)
		//TODO:: it should be 2
		expectedSealed = 1
		// rpcTimeout should be greater than snwatcher hb timeout(1s)
		rpcTimeout = 2 * time.Second
		// rate to cause intermittent failures
		mrReqFailRate = 30
	)

	type testEnv struct {
		mrfail      atomic.Bool
		failedsnid  atomic.Int32
		sealcnt     atomic.Int32
		sealedlsids sync.Map
		normallsids set.Set
		victimlsids set.Set
	}

	tcs := []struct {
		name  string
		testf func(t *testing.T, env *testEnv)
	}{
		{
			name: "ExpectedSeal",
			testf: func(t *testing.T, env *testEnv) {
				// Make victimsn failed
				env.failedsnid.Store(int32(victimsnid))

				require.Eventually(t, func() bool {
					return env.sealcnt.Load() >= int32(env.victimlsids.Size()+env.normallsids.Size())
				}, 30*time.Second, time.Second)

				var numSealed int
				env.sealedlsids.Range(func(k, _ any) bool {
					numSealed++
					require.True(t, env.victimlsids.Contains(k.(types.LogStreamID)))
					return true
				})

				require.Equal(t, expectedSealed, numSealed)
			},
		},
		{
			name: "UnexpectedSeal_DuringMRFail",
			testf: func(t *testing.T, env *testEnv) {
				// Make victimsn failed
				env.failedsnid.Store(int32(victimsnid))

				require.Eventually(t, func() bool {
					return env.sealcnt.Load() >= int32(env.victimlsids.Size())
				}, 30*time.Second, time.Second)

				// Make metarepos.ClusterMetadata failed
				env.mrfail.Store(true)

				// TODO:: unexpected sealed should be zero
				require.Eventually(t, func() bool {
					unexpectedSealed := 0
					env.normallsids.Foreach(func(f interface{}) bool {
						lsid := f.(types.LogStreamID)
						if _, ok := env.sealedlsids.Load(lsid); ok {
							unexpectedSealed++
						}
						return true
					})

					return env.normallsids.Size() == unexpectedSealed
				}, 60*time.Second, time.Second)

			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			victimsnpath := filepath.Join("/tmp", volume.StorageNodeDirName(cid, victimsnid))
			normalsnpath := filepath.Join("/tmp", volume.StorageNodeDirName(cid, normalsnid))
			createTime := time.Now().UTC()

			victimlsids := set.New(2)
			victimlsids.Add(victimlsid1)
			victimlsids.Add(victimlsid2)

			normallsids := set.New(2)
			normallsids.Add(normallsid1)
			normallsids.Add(normallsid2)

			env := &testEnv{
				victimlsids: victimlsids,
				normallsids: normallsids,
			}

			victimStorageNodeMetadata := &snpb.StorageNodeMetadataDescriptor{
				ClusterID: cid,
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: victimsnid,
				},
				LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
					{
						LogStreamReplica: varlogpb.LogStreamReplica{
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: victimsnid,
							},
							TopicLogStream: varlogpb.TopicLogStream{
								TopicID:     tpid,
								LogStreamID: victimlsid1,
							},
						},
					},
					{
						LogStreamReplica: varlogpb.LogStreamReplica{
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: victimsnid,
							},
							TopicLogStream: varlogpb.TopicLogStream{
								TopicID:     tpid,
								LogStreamID: victimlsid2,
							},
						},
					},
				},
				StartTime: createTime,
			}

			normalStorageNodeMetadata := &snpb.StorageNodeMetadataDescriptor{
				ClusterID: cid,
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: normalsnid,
				},
				LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
					{
						LogStreamReplica: varlogpb.LogStreamReplica{
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: normalsnid,
							},
							TopicLogStream: varlogpb.TopicLogStream{
								TopicID:     tpid,
								LogStreamID: normallsid1,
							},
						},
					},
					{
						LogStreamReplica: varlogpb.LogStreamReplica{
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: normalsnid,
							},
							TopicLogStream: varlogpb.TopicLogStream{
								TopicID:     tpid,
								LogStreamID: normallsid2,
							},
						},
					},
				},
				StartTime: createTime,
			}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).DoAndReturn(
				func(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
					if ctx.Err() != nil {
						return nil, ctx.Err()
					}

					if env.mrfail.Load() && rand.Int32N(100) < mrReqFailRate {
						return nil, context.Canceled
					}

					return &varlogpb.MetadataDescriptor{
						StorageNodes: []*varlogpb.StorageNodeDescriptor{
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: victimsnid,
								},
								CreateTime: createTime,
							},
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: normalsnid,
								},
								CreateTime: createTime,
							},
						},
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: victimlsid1,
								Status:      varlogpb.LogStreamStatusRunning,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   victimsnid,
										StorageNodePath: victimsnpath,
									},
								},
							},
							{
								TopicID:     tpid,
								LogStreamID: victimlsid2,
								Status:      varlogpb.LogStreamStatusRunning,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   victimsnid,
										StorageNodePath: victimsnpath,
									},
								},
							},
							{
								TopicID:     tpid,
								LogStreamID: normallsid1,
								Status:      varlogpb.LogStreamStatusRunning,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   normalsnid,
										StorageNodePath: normalsnpath,
									},
								},
							},
							{
								TopicID:     tpid,
								LogStreamID: normallsid2,
								Status:      varlogpb.LogStreamStatusRunning,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   normalsnid,
										StorageNodePath: normalsnpath,
									},
								},
							},
						},
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								LogStreams: []types.LogStreamID{
									victimlsid1,
									victimlsid2,
									normallsid1,
									normallsid2,
								},
							},
						},
					}, nil
				},
			).AnyTimes()

			mock.MockStorageNodeManager.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, snid types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error) {
					if types.StorageNodeID(env.failedsnid.Load()) == snid {
						time.Sleep(rpcTimeout)
						return nil, context.DeadlineExceeded
					}

					if snid == normalsnid {
						return normalStorageNodeMetadata, nil
					}
					return victimStorageNodeMetadata, nil
				},
			).AnyTimes()

			mock.MockStorageNodeManager.EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ types.TopicID, lsid types.LogStreamID, _ types.GLSN) ([]snpb.LogStreamReplicaMetadataDescriptor, error) {
					if ctx.Err() != nil {
						return nil, ctx.Err()
					}

					if victimlsids.Contains(lsid) {
						time.Sleep(rpcTimeout)
						return nil, context.DeadlineExceeded
					}

					return nil, nil
				},
			).AnyTimes()

			mock.MockMetadataRepositoryManager.EXPECT().Seal(gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, lsid types.LogStreamID) (types.GLSN, error) {
					env.sealcnt.Add(1)

					if ctx.Err() != nil {
						return types.InvalidGLSN, ctx.Err()
					}

					env.sealedlsids.Store(lsid, struct{}{})
					return types.GLSN(1), nil
				},
			).AnyTimes()

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithStatisticsRepository(mock.MockRepository),
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			// Wait for registering a storage node
			require.Eventually(t, func() bool {
				vsnm, _ := client.GetStorageNode(context.Background(), normalsnid)
				nsnm, _ := client.GetStorageNode(context.Background(), victimsnid)

				return len(vsnm.LogStreamReplicas) == victimlsids.Size() &&
					len(nsnm.LogStreamReplicas) == normallsids.Size()
			}, time.Second, 10*time.Millisecond)

			tc.testf(t, env)
		})
	}
}

func TestAdmin_GetStorageNode_FailedStorageNode(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(1)
		tpid = types.TopicID(1)
		lsid = types.LogStreamID(1)

		tick             = 10 * time.Millisecond
		heartbeatTimeout = int(24 * time.Hour / tick)
		reportInterval   = int(24 * time.Hour / tick)
	)

	snpath := filepath.Join("/tmp", volume.StorageNodeDirName(cid, snid))
	createTime := time.Now().UTC()
	failed := int32(0)
	failReported := int32(0)

	storageNodeMetadata := &snpb.StorageNodeMetadataDescriptor{
		ClusterID: cid,
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: snid,
		},
		LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
			{
				LogStreamReplica: varlogpb.LogStreamReplica{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snid,
					},
					TopicLogStream: varlogpb.TopicLogStream{
						TopicID:     tpid,
						LogStreamID: lsid,
					},
				},
			},
		},
		StartTime: createTime,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := newTestMock(ctrl)
	mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
		&varlogpb.MetadataDescriptor{
			StorageNodes: []*varlogpb.StorageNodeDescriptor{
				{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snid,
					},
					CreateTime: createTime,
				},
			},
			LogStreams: []*varlogpb.LogStreamDescriptor{
				{
					TopicID:     tpid,
					LogStreamID: lsid,
					Status:      varlogpb.LogStreamStatusRunning,
					Replicas: []*varlogpb.ReplicaDescriptor{
						{
							StorageNodeID:   snid,
							StorageNodePath: snpath,
						},
					},
				},
			},
			Topics: []*varlogpb.TopicDescriptor{
				{
					TopicID:    tpid,
					LogStreams: []types.LogStreamID{lsid},
				},
			},
		}, nil,
	).AnyTimes()
	mock.MockStorageNodeManager.EXPECT().GetMetadata(gomock.Any(), snid).DoAndReturn(
		func(context.Context, types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error) {
			if atomic.LoadInt32(&failed) == 0 {
				return storageNodeMetadata, nil
			}
			atomic.CompareAndSwapInt32(&failReported, 0, 1)
			snm := proto.Clone(storageNodeMetadata).(*snpb.StorageNodeMetadataDescriptor)
			snm.LogStreamReplicas = nil
			return snm, nil
		},
	).AnyTimes()

	tadm := admin.TestNewClusterManager(t,
		admin.WithListenAddress("127.0.0.1:0"),
		admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
		admin.WithStorageNodeManager(mock.MockStorageNodeManager),
		admin.WithStorageNodeWatcherOptions(
			snwatcher.WithTick(tick),
			snwatcher.WithHeartbeatTimeout(heartbeatTimeout),
			snwatcher.WithReportInterval(reportInterval),
			snwatcher.WithStatisticsRepository(mock.MockRepository),
		),
	)
	tadm.Serve(t)
	defer tadm.Close(t)

	client, closer := newTestClient(t, tadm.Address())
	defer closer()

	//  Wait for registering a storage node. (#12)
	require.Eventually(t, func() bool {
		_, err := client.GetStorageNode(context.Background(), snid)
		return err == nil
	}, time.Second, 10*time.Millisecond)

	snm, err := client.GetStorageNode(context.Background(), snid)
	assert.NoError(t, err)
	assert.NotEmpty(t, snm.LogStreamReplicas)

	time.Sleep(10 * tick)
	atomic.StoreInt32(&failed, 1)
	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&failReported) > 0
	}, 10*tick, tick)
	time.Sleep(10 * tick)

	snm, err = client.GetStorageNode(context.Background(), snid)
	assert.NoError(t, err)
	assert.NotEmpty(t, snm.LogStreamReplicas)
}

func TestAdmin_ListStorageNodes(t *testing.T) {
	const (
		snid = types.StorageNodeID(1)
		addr = "127.0.0.1:10000"
	)

	tcs := []struct {
		name    string
		success bool
		status  varlogpb.StorageNodeStatus
		prepare func(mock *testMock)
	}{
		{
			name:    "Success",
			success: true,
			status:  varlogpb.StorageNodeStatusRunning,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						StorageNodes: []*varlogpb.StorageNodeDescriptor{
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid,
									Address:       addr,
								},
								Status: varlogpb.StorageNodeStatusRunning,
							},
						},
					}, nil,
				).AnyTimes()
				mock.MockRepository.EXPECT().ListStorageNodes().Return(
					map[types.StorageNodeID]*admpb.StorageNodeMetadata{
						snid: {
							StorageNodeMetadataDescriptor: snpb.StorageNodeMetadataDescriptor{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid,
									Address:       addr,
								},
								Status: varlogpb.StorageNodeStatusRunning,
							},
						},
					},
				)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
					snwatcher.WithStatisticsRepository(mock.MockRepository),
				),
				admin.WithStatisticsRepository(mock.MockRepository),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			rsp, err := client.ListStorageNodes(context.Background())
			if tc.success {
				assert.NoError(t, err)
				assert.Equal(t, tc.status, rsp[0].Status)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_AddStorageNode(t *testing.T) {
	const (
		snid = types.StorageNodeID(1)
		addr = "127.0.0.1:10000"
	)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "AlreadyExistedNode",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockRepository.EXPECT().GetStorageNode(snid).Return(
					&admpb.StorageNodeMetadata{
						StorageNodeMetadataDescriptor: snpb.StorageNodeMetadataDescriptor{
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: snid,
								Address:       addr,
							},
						},
					}, true,
				)
			},
		},
		{
			name:    "GetMetadataError",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockRepository.EXPECT().GetStorageNode(snid).Return(nil, false)
				mock.MockStorageNodeManager.EXPECT().GetMetadataByAddress(gomock.Any(), snid, addr).Return(nil, errors.New("error"))
			},
		},
		{
			name:    "RejectedByMetadataRepository",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockRepository.EXPECT().GetStorageNode(snid).Return(nil, false)
				mock.MockStorageNodeManager.EXPECT().GetMetadataByAddress(gomock.Any(), snid, addr).Return(
					&snpb.StorageNodeMetadataDescriptor{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid,
							Address:       addr,
						},
						Storages: []varlogpb.StorageDescriptor{{Path: "/tmp"}},
					}, nil,
				)
				mock.MockMetadataRepositoryManager.EXPECT().RegisterStorageNode(gomock.Any(), gomock.Any()).Return(errors.New("error"))
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockStorageNodeManager.EXPECT().GetMetadataByAddress(gomock.Any(), snid, addr).Return(
					&snpb.StorageNodeMetadataDescriptor{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid,
							Address:       addr,
						},
						Storages: []varlogpb.StorageDescriptor{{Path: "/tmp"}},
					}, nil,
				)
				mock.MockMetadataRepositoryManager.EXPECT().RegisterStorageNode(gomock.Any(), gomock.Any()).Return(nil)
				mock.MockStorageNodeManager.EXPECT().AddStorageNode(gomock.Any(), snid, addr)
				mock.MockRepository.EXPECT().Report(gomock.Any(), gomock.Any(), gomock.Any())
				f := mock.MockRepository.EXPECT().GetStorageNode(snid).Return(nil, false)
				mock.MockRepository.EXPECT().GetStorageNode(snid).Return(
					&admpb.StorageNodeMetadata{
						StorageNodeMetadataDescriptor: snpb.StorageNodeMetadataDescriptor{
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: snid,
								Address:       addr,
							},
						},
					}, true,
				).After(f)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil).AnyTimes()

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithStatisticsRepository(mock.MockRepository),
				),
				admin.WithStatisticsRepository(mock.MockRepository),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			tc.prepare(mock)
			_, err := client.AddStorageNode(context.Background(), snid, addr)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_GetTopic(t *testing.T) {
	const tpid = types.TopicID(1)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "ClusterMetadataFetchError",
			success: false,
			prepare: func(mock *testMock) {
				// To create a new admin, ClusterMetadata should be called 3 times.
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).Times(3)
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					nil, errors.New("error"),
				)
			},
		},
		{
			name:    "NoSuchTopicID",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid + 1,
								Status:  varlogpb.TopicStatusRunning,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								Status:  varlogpb.TopicStatusRunning,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
					snwatcher.WithStatisticsRepository(mock.MockRepository),
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.GetTopic(context.Background(), tpid)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_ListTopics(t *testing.T) {
	const tpid = types.TopicID(1)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "ClusterMetadataFetchError",
			success: false,
			prepare: func(mock *testMock) {
				// To create a new admin, ClusterMetadata should be called 3 times.
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).Times(3)
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					nil, errors.New("error"),
				)
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								Status:  varlogpb.TopicStatusRunning,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
					snwatcher.WithStatisticsRepository(mock.MockRepository),
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.ListTopics(context.Background())
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_AddTopic(t *testing.T) {
	const tpid = types.TopicID(1)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "RejectedByMetadataRepository",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockMetadataRepositoryManager.EXPECT().RegisterTopic(gomock.Any(), tpid).Return(errors.New("error"))
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockMetadataRepositoryManager.EXPECT().RegisterTopic(gomock.Any(), tpid).Return(nil)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil).AnyTimes()

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			tc.prepare(mock)
			_, err := client.AddTopic(context.Background())
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_UnregisterTopic(t *testing.T) {
	const tpid = types.TopicID(1)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "ClusterMetadataFetchError",
			success: false,
			prepare: func(mock *testMock) {
				// To create a new admin, ClusterMetadata should be called 3 times.
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).Times(3)
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					nil, errors.New("error"),
				)
			},
		},
		{
			name:    "NoSuchTopicID",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid + 1,
								Status:  varlogpb.TopicStatusRunning,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "AlreadyDeleted",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								Status:  varlogpb.TopicStatusDeleted,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "RejectedByMetadataRepository",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								Status:  varlogpb.TopicStatusRunning,
							},
						},
					}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().UnregisterTopic(gomock.Any(), tpid).Return(errors.New("error"))
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								Status:  varlogpb.TopicStatusRunning,
							},
						},
					}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().UnregisterTopic(gomock.Any(), tpid).Return(nil)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			err := client.UnregisterTopic(context.Background(), tpid)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_GetLogStream(t *testing.T) {
	const (
		tpid = types.TopicID(1)
		lsid = types.LogStreamID(1)
	)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "ClusterMetadataFetchError",
			success: false,
			prepare: func(mock *testMock) {
				// To create a new admin, ClusterMetadata should be called 3 times.
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).Times(3)
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					nil, errors.New("error"),
				)
			},
		},
		{
			name:    "NoSuchTopicID",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid + 1,
								LogStreams: []types.LogStreamID{
									lsid,
								},
							},
						},
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "NoSuchLogStreamIDInTopic",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								LogStreams: []types.LogStreamID{
									lsid + 1,
								},
							},
						},
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "NoSuchLogStreamID",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								LogStreams: []types.LogStreamID{
									lsid,
								},
							},
						},
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid + 1,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "DifferentTopicID",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								LogStreams: []types.LogStreamID{
									lsid,
								},
							},
						},
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid + 1,
								LogStreamID: lsid,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								LogStreams: []types.LogStreamID{
									lsid,
								},
							},
						},
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.GetLogStream(context.Background(), tpid, lsid)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_ListLogStreams(t *testing.T) {
	const (
		tpid = types.TopicID(1)
		lsid = types.LogStreamID(1)
	)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "ClusterMetadataFetchError",
			success: false,
			prepare: func(mock *testMock) {
				// To create a new admin, ClusterMetadata should be called 3 times.
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).Times(3)
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					nil, errors.New("error"),
				)
			},
		},
		{
			name:    "NoSuchTopicID",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid + 1,
								LogStreams: []types.LogStreamID{
									lsid,
								},
							},
						},
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid + 1,
								LogStreamID: lsid,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "UnexpectedTopidID",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								LogStreams: []types.LogStreamID{
									lsid,
								},
							},
						},
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid + 1,
								LogStreamID: lsid,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "IgnoreMissingLogStreams",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								LogStreams: []types.LogStreamID{
									lsid,
								},
							},
						},
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid + 1,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						Topics: []*varlogpb.TopicDescriptor{
							{
								TopicID: tpid,
								LogStreams: []types.LogStreamID{
									lsid,
								},
							},
						},
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.ListLogStreams(context.Background(), tpid)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_AddLogStream(t *testing.T) {
	const (
		replicationFactor = 2
		cid               = types.ClusterID(1)
		tpid              = types.TopicID(1)
		lsid              = types.LogStreamID(1)
		snid1             = types.StorageNodeID(1)
		snid2             = types.StorageNodeID(2)
	)
	var (
		snpath1 = filepath.Join("/tmp", volume.StorageNodeDirName(cid, snid1))
		snpath2 = filepath.Join("/tmp", volume.StorageNodeDirName(cid, snid2))
	)

	tcs := []struct {
		name       string
		success    bool
		replicas   []*varlogpb.ReplicaDescriptor
		autoUnseal bool
		prepare    func(mock *testMock)
	}{
		{
			name:    "ReplicaSelectionError",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil).AnyTimes()
				mock.MockReplicaSelector.EXPECT().Select(gomock.Any()).Return(nil, errors.New("error"))
			},
		},
		{
			name:    "WrongReplicationFactor",
			success: false,
			replicas: []*varlogpb.ReplicaDescriptor{
				{StorageNodeID: snid1, StorageNodePath: snpath1},
			},
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil).AnyTimes()
			},
		},
		{
			name:    "NoSuchStorageNode",
			success: false,
			replicas: []*varlogpb.ReplicaDescriptor{
				{StorageNodeID: snid1, StorageNodePath: snpath1},
				{StorageNodeID: snid2, StorageNodePath: snpath2},
			},
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						StorageNodes: []*varlogpb.StorageNodeDescriptor{
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid1,
									Address:       "127.0.0.1:10000",
								},
								Paths: []string{snpath1},
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "RejectedByStorageNodeManager",
			success: false,
			replicas: []*varlogpb.ReplicaDescriptor{
				{StorageNodeID: snid1, StorageNodePath: snpath1},
				{StorageNodeID: snid2, StorageNodePath: snpath2},
			},
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						StorageNodes: []*varlogpb.StorageNodeDescriptor{
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid1,
									Address:       "127.0.0.1:10000",
								},
								Paths: []string{snpath1},
							},
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid2,
									Address:       "127.0.0.1:10001",
								},
								Paths: []string{snpath2},
							},
						},
					}, nil,
				).AnyTimes()
				mock.MockStorageNodeManager.EXPECT().AddLogStream(gomock.Any(), gomock.Any()).Return(nil, errors.New("error"))
			},
		},
		{
			name:    "RejectedByMetadataRepository",
			success: false,
			replicas: []*varlogpb.ReplicaDescriptor{
				{StorageNodeID: snid1, StorageNodePath: snpath1},
				{StorageNodeID: snid2, StorageNodePath: snpath2},
			},
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						StorageNodes: []*varlogpb.StorageNodeDescriptor{
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid1,
									Address:       "127.0.0.1:10000",
								},
								Paths: []string{snpath1},
							},
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid2,
									Address:       "127.0.0.1:10001",
								},
								Paths: []string{snpath2},
							},
						},
					}, nil,
				).AnyTimes()
				mock.MockStorageNodeManager.EXPECT().AddLogStream(gomock.Any(), gomock.Any()).Return(&varlogpb.LogStreamDescriptor{}, nil)
				mock.MockMetadataRepositoryManager.EXPECT().RegisterLogStream(gomock.Any(), gomock.Any()).Return(errors.New("error"))
			},
		},
		{
			name:    "Success",
			success: true,
			replicas: []*varlogpb.ReplicaDescriptor{
				{StorageNodeID: snid1, StorageNodePath: snpath1},
				{StorageNodeID: snid2, StorageNodePath: snpath2},
			},
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						StorageNodes: []*varlogpb.StorageNodeDescriptor{
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid1,
									Address:       "127.0.0.1:10000",
								},
								Paths: []string{snpath1},
							},
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid2,
									Address:       "127.0.0.1:10001",
								},
								Paths: []string{snpath2},
							},
						},
					}, nil,
				).AnyTimes()
				mock.MockStorageNodeManager.EXPECT().AddLogStream(gomock.Any(), gomock.Any()).Return(&varlogpb.LogStreamDescriptor{}, nil)
				mock.MockMetadataRepositoryManager.EXPECT().RegisterLogStream(gomock.Any(), gomock.Any()).Return(nil)

				// for sealed
				mock.MockRepository.EXPECT().GetLogStream(gomock.Any()).Return(
					stats.NewLogStreamStat(varlogpb.LogStreamStatusSealed, nil),
				)
				mock.MockRepository.EXPECT().SetLogStreamStatus(gomock.Any(), gomock.Any())

				// for unseal
				mock.MockStorageNodeManager.EXPECT().Unseal(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				mock.MockMetadataRepositoryManager.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			name:    "SuccessWithAutoUnseal",
			success: true,
			replicas: []*varlogpb.ReplicaDescriptor{
				{StorageNodeID: snid1, StorageNodePath: snpath1},
				{StorageNodeID: snid2, StorageNodePath: snpath2},
			},
			autoUnseal: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						StorageNodes: []*varlogpb.StorageNodeDescriptor{
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid1,
									Address:       "127.0.0.1:10000",
								},
								Paths: []string{snpath1},
							},
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid2,
									Address:       "127.0.0.1:10001",
								},
								Paths: []string{snpath2},
							},
						},
					}, nil,
				).Times(3)
				mock.MockStorageNodeManager.EXPECT().AddLogStream(gomock.Any(), gomock.Any()).Return(&varlogpb.LogStreamDescriptor{
					TopicID:     tpid,
					LogStreamID: lsid,
					Replicas: []*varlogpb.ReplicaDescriptor{
						{
							StorageNodeID:   snid1,
							StorageNodePath: snpath1,
						},
						{
							StorageNodeID:   snid2,
							StorageNodePath: snpath2,
						},
					},
				}, nil)
				mock.MockMetadataRepositoryManager.EXPECT().RegisterLogStream(gomock.Any(), gomock.Any()).Return(nil)
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						StorageNodes: []*varlogpb.StorageNodeDescriptor{
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid1,
									Address:       "127.0.0.1:10000",
								},
								Paths: []string{snpath1},
							},
							{
								StorageNode: varlogpb.StorageNode{
									StorageNodeID: snid2,
									Address:       "127.0.0.1:10001",
								},
								Paths: []string{snpath2},
							},
						},
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								LogStreamID: lsid,
								Status:      varlogpb.LogStreamStatusRunning,
							},
						},
					}, nil,
				).AnyTimes()

				mock.MockRepository.EXPECT().GetLogStream(gomock.Any()).Return(
					stats.NewLogStreamStat(varlogpb.LogStreamStatusRunning, nil),
				)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			opts := []admin.Option{
				admin.WithReplicationFactor(replicationFactor),
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithReplicaSelector(mock.MockReplicaSelector),
				admin.WithStatisticsRepository(mock.MockRepository),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
				),
			}
			if tc.autoUnseal {
				opts = append(opts, admin.WithAutoUnseal())
			}

			tadm := admin.TestNewClusterManager(t, opts...)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.AddLogStream(context.Background(), tpid, tc.replicas)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_UpdateLogStream(t *testing.T) {
	const (
		replicationFactor = 2
		cid               = types.ClusterID(1)
		tpid              = types.TopicID(1)
		lsid              = types.LogStreamID(1)
		snid1             = types.StorageNodeID(1)
		snid2             = types.StorageNodeID(2)
	)

	var (
		snpath1 = filepath.Join("/tmp", volume.StorageNodeDirName(cid, snid1))
		snpath2 = filepath.Join("/tmp", volume.StorageNodeDirName(cid, snid2))
	)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "ClusterMetadataFetchError",
			success: false,
			prepare: func(mock *testMock) {
				// To create a new admin, ClusterMetadata should be called 2 times - LogStreamIDGenerator and TopicIDGenerator.
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).Times(2)
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					nil, errors.New("error"),
				)
			},
		},
		{
			name: "NoSuchLogStreamID",
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid + 1,
								Status:      varlogpb.LogStreamStatusSealed,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name: "LogStreamUnexpectedStatus",
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
								Status:      varlogpb.LogStreamStatusRunning,
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name: "DoesNotHaveSealedReplica",
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
								Status:      varlogpb.LogStreamStatusSealed,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   snid1,
										StorageNodePath: snpath1,
									},
								},
							},
						},
					}, nil,
				).AnyTimes()
				mock.MockStorageNodeManager.EXPECT().GetMetadata(
					gomock.Any(), gomock.Any(),
				).Return(&snpb.StorageNodeMetadataDescriptor{
					LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
						{
							LogStreamReplica: varlogpb.LogStreamReplica{
								TopicLogStream: varlogpb.TopicLogStream{
									TopicID:     tpid,
									LogStreamID: lsid,
								},
							},
							Status: varlogpb.LogStreamStatusSealing,
						},
					},
				}, nil).AnyTimes()
			},
		},
		{
			name: "AddLogStreamReplicaError",
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
								Status:      varlogpb.LogStreamStatusSealed,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   snid1,
										StorageNodePath: snpath1,
									},
								},
							},
						},
					}, nil,
				).AnyTimes()

				mock.MockStorageNodeManager.EXPECT().GetMetadata(
					gomock.Any(), gomock.Any(),
				).Return(&snpb.StorageNodeMetadataDescriptor{
					LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
						{
							LogStreamReplica: varlogpb.LogStreamReplica{
								TopicLogStream: varlogpb.TopicLogStream{
									TopicID:     tpid,
									LogStreamID: lsid,
								},
							},
							Status: varlogpb.LogStreamStatusSealed,
						},
					},
				}, nil).AnyTimes()

				mock.MockStorageNodeManager.EXPECT().AddLogStreamReplica(
					gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
				).Return(snpb.LogStreamReplicaMetadataDescriptor{}, errors.New("error"))
			},
		},
		{
			name: "RejectedByMetadataRepository",
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
								Status:      varlogpb.LogStreamStatusSealed,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   snid1,
										StorageNodePath: snpath1,
									},
								},
							},
						},
					}, nil,
				).AnyTimes()

				mock.MockStorageNodeManager.EXPECT().GetMetadata(
					gomock.Any(), gomock.Any(),
				).Return(&snpb.StorageNodeMetadataDescriptor{
					LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
						{
							LogStreamReplica: varlogpb.LogStreamReplica{
								TopicLogStream: varlogpb.TopicLogStream{
									TopicID:     tpid,
									LogStreamID: lsid,
								},
							},
							Status: varlogpb.LogStreamStatusSealed,
						},
					},
				}, nil).AnyTimes()

				mock.MockStorageNodeManager.EXPECT().AddLogStreamReplica(
					gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
				).Return(snpb.LogStreamReplicaMetadataDescriptor{}, nil)

				mock.MockMetadataRepositoryManager.EXPECT().UpdateLogStream(
					gomock.Any(), gomock.Any(),
				).Return(errors.New("error"))

				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning)
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
								Status:      varlogpb.LogStreamStatusSealed,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   snid1,
										StorageNodePath: snpath1,
									},
								},
							},
						},
					}, nil,
				).AnyTimes()

				mock.MockStorageNodeManager.EXPECT().GetMetadata(
					gomock.Any(), gomock.Any(),
				).Return(&snpb.StorageNodeMetadataDescriptor{
					LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
						{
							LogStreamReplica: varlogpb.LogStreamReplica{
								TopicLogStream: varlogpb.TopicLogStream{
									TopicID:     tpid,
									LogStreamID: lsid,
								},
							},
							Status: varlogpb.LogStreamStatusSealed,
						},
					},
				}, nil).AnyTimes()

				mock.MockStorageNodeManager.EXPECT().AddLogStreamReplica(
					gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
				).Return(snpb.LogStreamReplicaMetadataDescriptor{}, nil)

				mock.MockMetadataRepositoryManager.EXPECT().UpdateLogStream(
					gomock.Any(), gomock.Any(),
				).Return(nil)

				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning)
			},
		},
		{
			name:    "AlreadyUpdated",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
								Status:      varlogpb.LogStreamStatusSealed,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   snid2,
										StorageNodePath: snpath2,
									},
								},
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithReplicationFactor(replicationFactor),
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStatisticsRepository(mock.MockRepository),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.UpdateLogStream(context.Background(), tpid, lsid,
				varlogpb.ReplicaDescriptor{ // pop (old)
					StorageNodeID:   snid1,
					StorageNodePath: snpath1,
				},
				varlogpb.ReplicaDescriptor{ // push (new)
					StorageNodeID:   snid2,
					StorageNodePath: snpath2,
				},
			)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_UnregisterLogStream(t *testing.T) {
	t.Skip()
}

func TestAdmin_RemoveLogStreamReplica(t *testing.T) {
	const (
		snid = types.StorageNodeID(1)
		tpid = types.TopicID(1)
		lsid = types.LogStreamID(1)
		path = "/tmp"
	)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "ClusterMetadataFetchError",
			success: false,
			prepare: func(mock *testMock) {
				// To create a new admin, ClusterMetadata should be called 3 times.
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).Times(3)
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					nil, errors.New("error"),
				)
			},
		},
		{
			name:    "UnexpectedStatus",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
								Status:      varlogpb.LogStreamStatusRunning,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   snid,
										StorageNodePath: path,
									},
								},
							},
						},
					}, nil,
				).AnyTimes()
			},
		},
		{
			name:    "RejectedByStorageNodeManager",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
								Status:      varlogpb.LogStreamStatusRunning,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   snid + 1,
										StorageNodePath: path,
									},
								},
							},
						},
					}, nil,
				).AnyTimes()
				mock.MockStorageNodeManager.EXPECT().RemoveLogStreamReplica(
					gomock.Any(), snid, tpid, lsid,
				).Return(errors.New("error"))
			},
		},
		{
			name:    "AlreadyRemovedOrNotExisted",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						LogStreams: []*varlogpb.LogStreamDescriptor{},
					}, nil,
				).AnyTimes()
				mock.MockStorageNodeManager.EXPECT().RemoveLogStreamReplica(
					gomock.Any(), snid, tpid, lsid,
				).Return(nil)
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{
						LogStreams: []*varlogpb.LogStreamDescriptor{
							{
								TopicID:     tpid,
								LogStreamID: lsid,
								Status:      varlogpb.LogStreamStatusRunning,
								Replicas: []*varlogpb.ReplicaDescriptor{
									{
										StorageNodeID:   snid + 1,
										StorageNodePath: path,
									},
								},
							},
						},
					}, nil,
				).AnyTimes()
				mock.MockStorageNodeManager.EXPECT().RemoveLogStreamReplica(
					gomock.Any(), snid, tpid, lsid,
				).Return(nil)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			err := client.RemoveLogStreamReplica(context.Background(), snid, tpid, lsid)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_Seal(t *testing.T) {
	const (
		tpid = types.TopicID(1)
		lsid = types.LogStreamID(1)
	)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "RejectedByMetadataRepository",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, gomock.Any())
				f := mock.MockMetadataRepositoryManager.EXPECT().Seal(gomock.Any(), lsid).Return(types.InvalidGLSN, errors.New("error"))
				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning).After(f)
			},
		},
		{
			name:    "RejectedByStorageNodeManager",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, gomock.Any())
				mock.MockMetadataRepositoryManager.EXPECT().Seal(gomock.Any(), lsid).Return(types.InvalidGLSN, nil)
				f := mock.MockStorageNodeManager.EXPECT().Seal(gomock.Any(), tpid, lsid, gomock.Any()).Return(nil, errors.New("error"))
				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning).After(f)
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, gomock.Any())
				mock.MockMetadataRepositoryManager.EXPECT().Seal(gomock.Any(), lsid).Return(types.InvalidGLSN, nil)
				mock.MockStorageNodeManager.EXPECT().Seal(gomock.Any(), tpid, lsid, gomock.Any()).Return(nil, nil)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStatisticsRepository(mock.MockRepository),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.Seal(context.Background(), tpid, lsid)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_Unseal(t *testing.T) {
	const (
		tpid = types.TopicID(1)
		lsid = types.LogStreamID(1)
	)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "RejectedByStorageNodeManager",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, varlogpb.LogStreamStatusUnsealing)
				f := mock.MockStorageNodeManager.EXPECT().Unseal(gomock.Any(), tpid, lsid).Return(errors.New("error"))
				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning).After(f)
			},
		},
		{
			name:    "RejectedByMetadataRepository",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, varlogpb.LogStreamStatusUnsealing)
				mock.MockStorageNodeManager.EXPECT().Unseal(gomock.Any(), tpid, lsid).Return(nil)
				f := mock.MockMetadataRepositoryManager.EXPECT().Unseal(gomock.Any(), lsid).Return(errors.New("error"))
				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning).After(f)
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockRepository.EXPECT().SetLogStreamStatus(lsid, varlogpb.LogStreamStatusUnsealing)
				mock.MockStorageNodeManager.EXPECT().Unseal(gomock.Any(), tpid, lsid).Return(nil)
				mock.MockMetadataRepositoryManager.EXPECT().Unseal(gomock.Any(), lsid).Return(nil)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStatisticsRepository(mock.MockRepository),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.Unseal(context.Background(), tpid, lsid)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_Sync(t *testing.T) {
	const (
		tpid  = types.TopicID(1)
		lsid  = types.LogStreamID(1)
		srcid = types.StorageNodeID(1)
		dstid = types.StorageNodeID(2)
	)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "RejectedByMetadataRepository",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().Seal(gomock.Any(), lsid).Return(types.InvalidGLSN, errors.New("error"))
			},
		},
		{
			name:    "RejectedByStorageNodeManager",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().Seal(gomock.Any(), lsid).Return(types.MinGLSN, nil)
				mock.MockStorageNodeManager.EXPECT().Sync(gomock.Any(), tpid, lsid, srcid, dstid, gomock.Any()).Return(nil, errors.New("error"))
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().Seal(gomock.Any(), lsid).Return(types.MinGLSN, nil)
				mock.MockStorageNodeManager.EXPECT().Sync(gomock.Any(), tpid, lsid, srcid, dstid, gomock.Any()).Return(&snpb.SyncStatus{}, nil)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.Sync(context.Background(), tpid, lsid, srcid, dstid)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// See VARLOG-716.
func TestAdmin_DoNotSyncSealedReplicas_(t *testing.T) {
	const (
		sealedGLSN = types.GLSN(5)

		snid1    = types.StorageNodeID(1)
		addr1    = "127.0.0.1:10000"
		version1 = types.Version(1)
		ghwm1    = types.GLSN(10)

		snid2    = types.StorageNodeID(2)
		addr2    = "127.0.0.1:10001"
		version2 = types.Version(2)
		ghwm2    = types.GLSN(20)

		tick           = 10 * time.Millisecond
		reportInterval = 5
	)

	var (
		metadata = &varlogpb.MetadataDescriptor{
			StorageNodes: []*varlogpb.StorageNodeDescriptor{
				{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snid1,
						Address:       addr1,
					},
				},
				{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snid2,
						Address:       addr2,
					},
				},
			},
		}
		lss = stats.NewLogStreamStat(
			varlogpb.LogStreamStatusSealed,
			map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor{
				snid1: {
					LogStreamReplica: varlogpb.LogStreamReplica{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid1,
						},
					},
					Status:              varlogpb.LogStreamStatusSealed,
					Version:             version1,
					GlobalHighWatermark: ghwm1,
				},
				snid2: {
					LogStreamReplica: varlogpb.LogStreamReplica{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid2,
						},
					},
					Status:              varlogpb.LogStreamStatusSealed,
					Version:             version2,
					GlobalHighWatermark: ghwm2,
				},
			},
		)
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := newTestMock(ctrl)

	mock.MockMetadataRepositoryManager.EXPECT().Seal(gomock.Any(), gomock.Any()).Return(sealedGLSN, nil).AnyTimes()
	mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(metadata, nil).AnyTimes()
	mock.MockStorageNodeManager.EXPECT().GetMetadata(gomock.Any(), snid1).DoAndReturn(
		func(context.Context, types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error) {
			snd := metadata.GetStorageNode(snid1)
			assert.NotNil(t, snd)

			lsrmd, ok := lss.Replica(snid1)
			assert.True(t, ok)

			return &snpb.StorageNodeMetadataDescriptor{
				StorageNode:       snd.StorageNode,
				LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{lsrmd},
			}, nil
		},
	).MinTimes(1)
	mock.MockStorageNodeManager.EXPECT().GetMetadata(gomock.Any(), snid2).DoAndReturn(
		func(context.Context, types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error) {
			snd := metadata.GetStorageNode(snid2)
			assert.NotNil(t, snd)

			lsrmd, ok := lss.Replica(snid2)
			assert.True(t, ok)

			return &snpb.StorageNodeMetadataDescriptor{
				StorageNode:       snd.StorageNode,
				LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{lsrmd},
			}, nil
		},
	).MinTimes(1)

	mock.MockRepository.EXPECT().Report(gomock.Any(), gomock.Any(), gomock.Any()).Return().AnyTimes()
	mock.MockRepository.EXPECT().GetLogStream(gomock.Any()).Return(lss).AnyTimes()

	tadm := admin.TestNewClusterManager(t,
		admin.WithListenAddress("127.0.0.1:0"),
		admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
		admin.WithStorageNodeManager(mock.MockStorageNodeManager),
		admin.WithStatisticsRepository(mock.MockRepository),
		admin.WithStorageNodeWatcherOptions(
			snwatcher.WithTick(tick),
			snwatcher.WithReportInterval(reportInterval),
			snwatcher.WithHeartbeatTimeout(math.MaxInt), // no heartbeat checking
		),
		admin.WithLogStreamGCTimeout(math.MaxInt64), // no log stream gc
	)
	tadm.Serve(t)
	defer tadm.Close(t)

	time.Sleep(tick * reportInterval * 10)
}

func TestAdmin_Trim(t *testing.T) {
	const tpid = types.TopicID(1)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "RejectedByStorageNodeManager",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockStorageNodeManager.EXPECT().Trim(gomock.Any(), tpid, gomock.Any()).Return(nil, errors.New("error"))
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockStorageNodeManager.EXPECT().Trim(gomock.Any(), tpid, gomock.Any()).Return(nil, nil)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.Trim(context.Background(), tpid, types.GLSN(10))
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_GetMetadataRepositoryNode(t *testing.T) {
	nid := types.NewNodeID("127.0.0.1:10000")

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "GetClusterInfoError",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().GetClusterInfo(gomock.Any()).Return(
					nil, errors.New("error"),
				)
			},
		},
		{
			name:    "NoSuchNodeID",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().GetClusterInfo(gomock.Any()).Return(
					&mrpb.ClusterInfo{
						NodeID: nid + 1,
						Leader: nid + 1,
						Members: map[types.NodeID]*mrpb.ClusterInfo_Member{
							nid + 1: {
								Peer:     "http://127.0.0.1:20000",
								Endpoint: "127.0.0.1:20001",
								Learner:  false,
							},
						},
					}, nil,
				)
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().GetClusterInfo(gomock.Any()).Return(
					&mrpb.ClusterInfo{
						NodeID: nid,
						Leader: nid,
						Members: map[types.NodeID]*mrpb.ClusterInfo_Member{
							nid: {
								Peer:     "http://127.0.0.1:10000",
								Endpoint: "127.0.0.1:10001",
								Learner:  false,
							},
						},
					}, nil,
				)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.GetMetadataRepositoryNode(context.Background(), nid)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_ListMetadataRepositoryNodes(t *testing.T) {
	nid := types.NewNodeID("127.0.0.1:10000")

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
	}{
		{
			name:    "GetClusterInfoError",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().GetClusterInfo(gomock.Any()).Return(
					nil, errors.New("error"),
				)
			},
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().GetClusterInfo(gomock.Any()).Return(
					&mrpb.ClusterInfo{
						NodeID: nid,
						Leader: nid,
						Members: map[types.NodeID]*mrpb.ClusterInfo_Member{
							nid: {
								Peer:     "http://127.0.0.1:10000",
								Endpoint: "127.0.0.1:10001",
								Learner:  false,
							},
						},
					}, nil,
				)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.ListMetadataRepositoryNodes(context.Background())
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdmin_AddMetadataRepositoryNode(t *testing.T) {
	const (
		raftURL = "https://127.0.0.1:10000"
		rpcAddr = "127.0.0.1:10001"
	)

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
		raftURL string
	}{
		{
			name:    "InvalidNodeID",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
			},
			raftURL: "https://" + types.InvalidNodeID.Reverse(),
		},
		{
			name:    "RejectedByMetadataRepository",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().AddPeer(
					gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
				).Return(errors.New("error"))
			},
			raftURL: raftURL,
		},
		{
			name:    "AlreadyExistedNode",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().AddPeer(
					gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
				).Return(verrors.ErrAlreadyExists)
			},
			raftURL: raftURL,
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().AddPeer(
					gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
				).Return(nil)
			},
			raftURL: raftURL,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			_, err := client.AddMetadataRepositoryNode(context.Background(), tc.raftURL, rpcAddr)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
func TestAdmin_DeleteMetadataRepositoryNode(t *testing.T) {
	nid := types.NewNodeID("127.0.0.1:10000")

	tcs := []struct {
		name    string
		success bool
		prepare func(mock *testMock)
		nid     types.NodeID
	}{
		{
			name:    "InvalidNodeID",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
			},
			nid: types.InvalidNodeID,
		},
		{
			name:    "RejectedByMetadataRepository",
			success: false,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().RemovePeer(
					gomock.Any(), gomock.Any(),
				).Return(errors.New("error"))
			},
			nid: nid,
		},
		{
			name:    "Success",
			success: true,
			prepare: func(mock *testMock) {
				mock.MockClusterMetadataView.EXPECT().ClusterMetadata(gomock.Any()).Return(
					&varlogpb.MetadataDescriptor{}, nil,
				).AnyTimes()
				mock.MockMetadataRepositoryManager.EXPECT().RemovePeer(
					gomock.Any(), gomock.Any(),
				).Return(nil)
			},
			nid: nid,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := newTestMock(ctrl)
			tc.prepare(mock)

			tadm := admin.TestNewClusterManager(t,
				admin.WithListenAddress("127.0.0.1:0"),
				admin.WithMetadataRepositoryManager(mock.MockMetadataRepositoryManager),
				admin.WithStorageNodeManager(mock.MockStorageNodeManager),
				admin.WithStorageNodeWatcherOptions(
					snwatcher.WithTick(time.Hour), // no heartbeat checking
				),
			)
			tadm.Serve(t)
			defer tadm.Close(t)

			client, closer := newTestClient(t, tadm.Address())
			defer closer()

			err := client.DeleteMetadataRepositoryNode(context.Background(), tc.nid)
			if tc.success {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
