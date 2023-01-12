package varlogctl_test

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/internal/varlogctl"
	"github.com/kakao/varlog/internal/varlogctl/logstream"
	"github.com/kakao/varlog/internal/varlogctl/metarepos"
	"github.com/kakao/varlog/internal/varlogctl/storagenode"
	"github.com/kakao/varlog/internal/varlogctl/topic"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/admpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/testdata"
)

var update = flag.Bool("update", false, "update files")

const (
	cid = types.ClusterID(1)

	tpid1 = types.TopicID(1)
	lsid1 = types.LogStreamID(1)
	lsid2 = types.LogStreamID(2)

	snid1 = types.StorageNodeID(1)
	addr1 = "127.0.0.1:10000"

	snid2 = types.StorageNodeID(2)
	addr2 = "127.0.0.2:10000"

	rafturl1 = "http://127.0.1.1:10000"
	rpcaddr1 = "127.0.1.1:10001"
)

var (
	lsrmd1 = &snpb.LogStreamReplicaMetadataDescriptor{
		LogStreamReplica: varlogpb.LogStreamReplica{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid1,
				Address:       addr1,
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     tpid1,
				LogStreamID: lsid1,
			},
		},
		Status:              varlogpb.LogStreamStatusRunning,
		Version:             types.Version(1),
		GlobalHighWatermark: types.GLSN(100),
		LocalLowWatermark: varlogpb.LogSequenceNumber{
			LLSN: types.LLSN(1),
			GLSN: types.GLSN(1),
		},
		LocalHighWatermark: varlogpb.LogSequenceNumber{
			LLSN: types.LLSN(51),
			GLSN: types.GLSN(97),
		},
		Path:             "/tmp1/foo",
		StorageSizeBytes: 4096,
	}

	lsrmd2 = &snpb.LogStreamReplicaMetadataDescriptor{
		LogStreamReplica: varlogpb.LogStreamReplica{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid2,
				Address:       addr2,
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     tpid1,
				LogStreamID: lsid1,
			},
		},
		Status:              varlogpb.LogStreamStatusRunning,
		Version:             types.Version(1),
		GlobalHighWatermark: types.GLSN(100),
		LocalLowWatermark: varlogpb.LogSequenceNumber{
			LLSN: types.LLSN(1),
			GLSN: types.GLSN(1),
		},
		LocalHighWatermark: varlogpb.LogSequenceNumber{
			LLSN: types.LLSN(51),
			GLSN: types.GLSN(97),
		},
		Path:             "/tmp1/foo",
		StorageSizeBytes: 4096,
	}

	snm1 = &admpb.StorageNodeMetadata{
		StorageNodeMetadataDescriptor: snpb.StorageNodeMetadataDescriptor{
			ClusterID: cid,
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid1,
				Address:       addr1,
			},
			Storages: []varlogpb.StorageDescriptor{
				{
					Path:  "/tmp1",
					Used:  32 << 10,
					Total: 1 << 20,
				},
				{
					Path:  "/tmp2",
					Used:  64 << 10,
					Total: 2 << 20,
				},
			},
			LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{*lsrmd1},
			StartTime:         time.Date(2022, time.October, 1, 3, 23, 21, 0, time.UTC),
		},
		CreateTime:        time.Date(2022, time.September, 27, 17, 46, 40, 0, time.UTC),
		LastHeartbeatTime: time.Date(2022, time.November, 1, 11, 37, 19, 0, time.UTC),
	}

	snm2 = &admpb.StorageNodeMetadata{
		StorageNodeMetadataDescriptor: snpb.StorageNodeMetadataDescriptor{
			ClusterID: cid,
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid1,
				Address:       addr1,
			},
			Storages: []varlogpb.StorageDescriptor{
				{
					Path:  "/tmp1",
					Used:  32 << 10,
					Total: 1 << 20,
				},
				{
					Path:  "/tmp2",
					Used:  64 << 10,
					Total: 2 << 20,
				},
			},
			LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{},
			StartTime:         time.Date(2022, time.October, 1, 3, 23, 21, 0, time.UTC),
		},
		CreateTime:        time.Date(2022, time.September, 27, 17, 46, 40, 0, time.UTC),
		LastHeartbeatTime: time.Date(2022, time.November, 1, 11, 37, 19, 0, time.UTC),
	}

	td1 = &varlogpb.TopicDescriptor{
		TopicID: tpid1,
		Status:  varlogpb.TopicStatusRunning,
		LogStreams: []types.LogStreamID{
			lsid1,
			lsid2,
		},
	}

	lsd1 = &varlogpb.LogStreamDescriptor{
		TopicID:     tpid1,
		LogStreamID: lsid1,
		Status:      varlogpb.LogStreamStatusRunning,
		Replicas: []*varlogpb.ReplicaDescriptor{
			{
				StorageNodeID:   snid1,
				StorageNodePath: "/tmp",
			},
			{
				StorageNodeID:   snid2,
				StorageNodePath: "/tmp",
			},
		},
	}

	sealRsp = &admpb.SealResponse{
		LogStreams: []snpb.LogStreamReplicaMetadataDescriptor{*lsrmd1, *lsrmd2},
		SealedGLSN: types.GLSN(10),
	}

	mrnode1 = &varlogpb.MetadataRepositoryNode{
		NodeID:  types.NewNodeIDFromURL(rafturl1),
		RaftURL: rafturl1,
		RPCAddr: rpcaddr1,
		Leader:  true,
		Learner: false,
	}
)

func TestController(t *testing.T) {
	tcs := []struct {
		name        string
		golden      string
		executeFunc varlogctl.ExecuteFunc
		initMock    func(*varlog.MockAdmin)
	}{
		{
			name:        "GetStorageNode0",
			golden:      "varlogctl/getstoragenode.0.golden.json",
			executeFunc: storagenode.Describe(snm1.StorageNode.StorageNodeID),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().GetStorageNode(gomock.Any(), snm1.StorageNode.StorageNodeID).Return(snm1, nil)
			},
		},
		{
			name:        "GetStorageNode1",
			golden:      "varlogctl/getstoragenode.1.golden.json",
			executeFunc: storagenode.Describe(snm2.StorageNode.StorageNodeID),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().GetStorageNode(gomock.Any(), snm2.StorageNode.StorageNodeID).Return(snm2, nil)
			},
		},
		{
			name:        "ListStorageNodes0",
			golden:      "varlogctl/liststoragenodes.0.golden.json",
			executeFunc: storagenode.Describe(),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().ListStorageNodes(gomock.Any()).Return([]admpb.StorageNodeMetadata{}, nil)
			},
		},
		{
			name:        "ListStorageNodes1",
			golden:      "varlogctl/liststoragenodes.1.golden.json",
			executeFunc: storagenode.Describe(),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().ListStorageNodes(gomock.Any()).Return(
					[]admpb.StorageNodeMetadata{*snm1}, nil,
				)
			},
		},
		{
			name:        "AddStorageNode0",
			golden:      "varlogctl/addstoragenode.0.golden.json",
			executeFunc: storagenode.Add(snm1.StorageNode.Address, snm1.StorageNode.StorageNodeID),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().AddStorageNode(
					gomock.Any(),
					snm1.StorageNode.StorageNodeID,
					snm1.StorageNode.Address,
				).Return(snm1, nil)
			},
		},
		{
			name:        "UnregisterStorageNode0",
			golden:      "varlogctl/unregisterstoragenode.0.golden.json",
			executeFunc: storagenode.Remove(snm1.StorageNode.Address, snm1.StorageNode.StorageNodeID),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().UnregisterStorageNode(
					gomock.Any(),
					gomock.Any(),
				).Return(nil)
			},
		},
		{
			name:        "GetTopic0",
			golden:      "varlogctl/gettopic.0.golden.json",
			executeFunc: topic.Describe(td1.TopicID),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().GetTopic(gomock.Any(), td1.TopicID).Return(td1, nil)
			},
		},
		{
			name:        "ListTopics0",
			golden:      "varlogctl/listtopics.0.golden.json",
			executeFunc: topic.Describe(),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().ListTopics(gomock.Any()).Return([]varlogpb.TopicDescriptor{}, nil)
			},
		},
		{
			name:        "ListTopics1",
			golden:      "varlogctl/listtopics.1.golden.json",
			executeFunc: topic.Describe(),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().ListTopics(gomock.Any()).Return([]varlogpb.TopicDescriptor{*td1}, nil)
			},
		},
		{
			name:        "AddTopic",
			golden:      "varlogctl/addtopic.0.golden.json",
			executeFunc: topic.Add(),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().AddTopic(gomock.Any()).Return(td1, nil)
			},
		},
		{
			name:        "UnregisterTopic",
			golden:      "varlogctl/unregistertopic.0.golden.json",
			executeFunc: topic.Remove(td1.TopicID),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().UnregisterTopic(gomock.Any(), td1.TopicID).Return(nil)
			},
		},
		{
			name:        "GetLogStream",
			golden:      "varlogctl/getlogstream.0.golden.json",
			executeFunc: logstream.Describe(lsd1.TopicID, lsd1.LogStreamID),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().GetLogStream(gomock.Any(), lsd1.TopicID, lsd1.LogStreamID).Return(lsd1, nil)
			},
		},
		{
			name:        "ListLogStreams",
			golden:      "varlogctl/listlogstreams.0.golden.json",
			executeFunc: logstream.Describe(lsd1.TopicID),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().ListLogStreams(gomock.Any(), lsd1.TopicID).Return(
					[]varlogpb.LogStreamDescriptor{}, nil,
				)
			},
		},
		{
			name:        "ListLogStreams",
			golden:      "varlogctl/listlogstreams.1.golden.json",
			executeFunc: logstream.Describe(lsd1.TopicID),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().ListLogStreams(gomock.Any(), lsd1.TopicID).Return(
					[]varlogpb.LogStreamDescriptor{*lsd1}, nil,
				)
			},
		},
		{
			name:        "AddLogStream",
			golden:      "varlogctl/addlogstream.0.golden.json",
			executeFunc: logstream.Add(lsd1.TopicID),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().AddLogStream(gomock.Any(), lsd1.TopicID, nil).Return(lsd1, nil)
			},
		},
		/*
			{
				name:   "UpdateLogStream",
				golden: "varlogctl/updatelogstream.0.golden.json",
			},
		*/
		{
			name:        "Seal",
			golden:      "varlogctl/seal.0.golden.json",
			executeFunc: logstream.Seal(tpid1, lsid1),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any()).Return(sealRsp, nil)
			},
		},
		{
			name:        "Unseal",
			golden:      "varlogctl/unseal.0.golden.json",
			executeFunc: logstream.Unseal(tpid1, lsid1),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().Unseal(gomock.Any(), gomock.Any(), gomock.Any()).Return(lsd1, nil)
			},
		},
		{
			name:        "Sync",
			golden:      "varlogctl/sync.0.golden.json",
			executeFunc: logstream.Sync(tpid1, lsid1, snid1, snid2),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().Sync(gomock.Any(), tpid1, lsid1, snid1, snid2).Return(
					&snpb.SyncStatus{
						State: snpb.SyncStateInProgress,
						First: snpb.SyncPosition{
							LLSN: types.LLSN(1),
							GLSN: types.GLSN(1),
						},
						Last: snpb.SyncPosition{
							LLSN: types.LLSN(10),
							GLSN: types.GLSN(10),
						},
						Current: snpb.SyncPosition{
							LLSN: types.LLSN(5),
							GLSN: types.GLSN(5),
						},
					}, nil,
				)
			},
		},
		/*
			{
				name: "Trim",
			},
		*/
		{
			name:        "GetMetadataRepositoryNode",
			golden:      "varlogctl/getmetadatarepositorynode.0.golden.json",
			executeFunc: metarepos.Describe(rafturl1),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().GetMetadataRepositoryNode(gomock.Any(), types.NewNodeIDFromURL(rafturl1)).Return(
					mrnode1, nil,
				)
			},
		},
		{
			name:        "ListMetadataRepositoryNodes",
			golden:      "varlogctl/listmetadatarepositorynodes.0.golden.json",
			executeFunc: metarepos.Describe(),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().ListMetadataRepositoryNodes(gomock.Any()).Return(
					[]varlogpb.MetadataRepositoryNode{*mrnode1}, nil,
				)
			},
		},
		{
			name:        "AddMetadataRepositoryNode",
			golden:      "varlogctl/addmetadatarepositorynode.0.golden.json",
			executeFunc: metarepos.Add(rafturl1, rpcaddr1),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().AddMetadataRepositoryNode(gomock.Any(), rafturl1, rpcaddr1).Return(mrnode1, nil)
			},
		},
		{
			name:        "DeleteMetadataRepositoryNode",
			golden:      "varlogctl/deletemetadatarepositorynode.0.golden.json",
			executeFunc: metarepos.Remove(rafturl1),
			initMock: func(adm *varlog.MockAdmin) {
				adm.EXPECT().DeleteMetadataRepositoryNode(gomock.Any(), types.NewNodeIDFromURL(rafturl1)).Return(nil)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			adm := varlog.NewMockAdmin(ctrl)
			tc.initMock(adm)

			vc, err := varlogctl.New(
				varlogctl.WithAdmin(adm),
				varlogctl.WithExecuteFunc(tc.executeFunc),
			)
			assert.NoError(t, err)

			data, err := vc.Execute(context.Background())
			assert.NoError(t, err)

			got, err := vc.Decode(data)
			assert.NoError(t, err)

			path := testdata.Path(tc.golden)
			if *update {
				fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
				assert.NoError(t, err)

				err = vc.Print(got, fp)
				assert.NoError(t, err)

				err = fp.Close()
				assert.NoError(t, err)
				return
			}

			want, err := os.ReadFile(path)
			assert.NoError(t, err)

			if len(got) > 0 || len(want) > 0 {
				assert.Equal(t, want, got)
			}
		})
	}
}

/*
func testController(t *testing.T, admin varlog.Admin, executeFunc varlogctl.ExecuteFunc, resultCheck func(result result.Result)) {
	vc, err := varlogctl.New(
		varlogctl.WithExecuteFunc(executeFunc),
		varlogctl.WithAdmin(admin),
	)
	require.NoError(t, err)

	res, err := vc.Execute(context.Background())
	assert.NoError(t, err)

	var sb strings.Builder
	require.NoError(t, vc.Print(res, &sb))

	fn := runtime.FuncForPC(reflect.ValueOf(executeFunc).Pointer()).Name()
	toks := strings.Split(fn, "/")
	fn = toks[len(toks)-1]
	t.Logf("%s:\n%+v", fn, sb.String())

	r := result.Result{}
	require.NoError(t, json.Unmarshal([]byte(sb.String()), &r))
	resultCheck(r)
}

func TestMetadataRepository(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	admin := varlog.NewMockAdmin(ctrl)

	// Describe
	admin.EXPECT().GetMRMembers(gomock.Any()).Return(&admpb.GetMRMembersResponse{}, nil)
	testController(t, admin, metarepos.Describe(), func(res result.Result) {
		require.NoError(t, res.Err())
		require.Equal(t, 0, res.NumberOfDataItem())
	})

	// Describe
	admin.EXPECT().GetMRMembers(gomock.Any()).Return(&admpb.GetMRMembersResponse{
		Leader:            types.MinNodeID,
		ReplicationFactor: 3,
		Members: map[types.NodeID]string{
			types.NodeID(2): "https://foo",
		},
	}, nil).Times(2)
	testController(t, admin, metarepos.Describe("https://foo"), func(res result.Result) {
		require.NoError(t, res.Err())
		require.Equal(t, 1, res.NumberOfDataItem())
	})
	testController(t, admin, metarepos.Describe("https://bar"), func(res result.Result) {
		require.Error(t, res.Err())
		require.Equal(t, 0, res.NumberOfDataItem())
	})

	// Add
	admin.EXPECT().AddMRPeer(gomock.Any(), gomock.Any(), gomock.Any()).Return(types.NodeID(1), nil)
	testController(t, admin, metarepos.Add("https://foo", "foo.mr"), func(res result.Result) {
		require.NoError(t, res.Err())
	})
}

func TestStorageNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	admin := varlog.NewMockAdmin(ctrl)

	// Describe
	admin.EXPECT().GetStorageNodes(gomock.Any()).Return(nil, nil)
	testController(t, admin, storagenode.Describe(), func(res result.Result) {
		require.NoError(t, res.Err())
		require.Equal(t, 0, res.NumberOfDataItem())
	})

	admin.EXPECT().GetStorageNodes(gomock.Any()).Return(map[types.StorageNodeID]admpb.StorageNodeMetadata{
		types.StorageNodeID(1): {
			StorageNodeMetadataDescriptor: snpb.StorageNodeMetadataDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: types.StorageNodeID(1),
					Address:       "sn1",
				},
			},
		},
	}, nil)
	testController(t, admin, storagenode.Describe(), func(res result.Result) {
		require.NoError(t, res.Err())
		require.Equal(t, 1, res.NumberOfDataItem())
		item, ok := res.GetDataItem(0)
		require.True(t, ok)
		row, ok := item.(map[string]interface{})
		require.True(t, ok)
		require.EqualValues(t, types.StorageNodeID(1), row["storageNodeId"])
		require.EqualValues(t, "sn1", row["address"])
	})

	// Add
	admin.EXPECT().AddStorageNode(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&snpb.StorageNodeMetadataDescriptor{
			ClusterID: types.ClusterID(1),
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(1),
				Address:       "sn1",
			},
			Status: varlogpb.StorageNodeStatusRunning,
			Storages: []varlogpb.StorageDescriptor{
				{
					StorageNodePath: "/tmp/data1",
				},
			},
		}, nil)
	testController(t, admin, storagenode.Add("sn1", types.StorageNodeID(1)), func(res result.Result) {
		require.NoError(t, res.Err())
		require.Equal(t, 1, res.NumberOfDataItem())
		item, ok := res.GetDataItem(0)
		require.True(t, ok)
		row, ok := item.(map[string]interface{})
		require.True(t, ok)

		sn, ok := row["storageNode"].(map[string]interface{})
		require.True(t, ok)

		require.EqualValues(t, types.StorageNodeID(1), sn["storageNodeId"])
		require.EqualValues(t, "sn1", sn["address"])
	})
}

func TestTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	admin := varlog.NewMockAdmin(ctrl)

	// Describe: list
	admin.EXPECT().ListTopics(gomock.Any()).Return(nil, nil)
	testController(t, admin, topic.Describe(), func(res result.Result) {
		require.NoError(t, res.Err())
		require.Equal(t, 0, res.NumberOfDataItem())
	})

	// Describe: detail
	td := varlogpb.TopicDescriptor{
		TopicID: types.TopicID(1),
		Status:  varlogpb.TopicStatusRunning,
		LogStreams: []types.LogStreamID{
			types.LogStreamID(1),
			types.LogStreamID(2),
		},
	}
	admin.EXPECT().ListTopics(gomock.Any()).Return([]varlogpb.TopicDescriptor{td}, nil).Times(2)
	testController(t, admin, topic.Describe(), func(res result.Result) {
		require.NoError(t, res.Err())
		require.Equal(t, 1, res.NumberOfDataItem())
		item, ok := res.GetDataItem(0)
		require.True(t, ok)
		row, ok := item.(map[string]interface{})
		require.True(t, ok)
		require.EqualValues(t, row["topicId"], td.TopicID)
	})
	// Describe: no such topic
	testController(t, admin, topic.Describe(td.TopicID+1), func(res result.Result) {
		require.Error(t, res.Err())
		require.Equal(t, 0, res.NumberOfDataItem())
	})

	// Add
	td.LogStreams = nil
	admin.EXPECT().AddTopic(gomock.Any()).Return(td, nil)
	testController(t, admin, topic.Add(), func(res result.Result) {
		require.NoError(t, res.Err())
		require.Equal(t, 1, res.NumberOfDataItem())
		item, ok := res.GetDataItem(0)
		require.True(t, ok)
		row, ok := item.(map[string]interface{})
		require.True(t, ok)
		require.EqualValues(t, row["topicId"], td.TopicID)
	})

	// Remove
	admin.EXPECT().UnregisterTopic(gomock.Any(), gomock.Any()).Return(nil)
	testController(t, admin, topic.Remove(types.TopicID(1)), func(res result.Result) {
		require.NoError(t, res.Err())
		require.Equal(t, 0, res.NumberOfDataItem())
	})
}

func TestLogStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	admin := varlog.NewMockAdmin(ctrl)

	admin.EXPECT().AddLogStream(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&varlogpb.LogStreamDescriptor{
			TopicID:     types.TopicID(1),
			LogStreamID: types.LogStreamID(1),
			Status:      varlogpb.LogStreamStatusRunning,
			Replicas: []*varlogpb.ReplicaDescriptor{
				{
					StorageNodeID: types.StorageNodeID(1),
					StorageNodePath:          "/tmp/data1",
				},
			},
		}, nil,
	)
	testController(t, admin, logstream.Add(types.TopicID(1)), func(res result.Result) {
		require.NoError(t, res.Err())
		require.Equal(t, 1, res.NumberOfDataItem())
		item, ok := res.GetDataItem(0)
		require.True(t, ok)
		row, ok := item.(map[string]interface{})
		require.True(t, ok)
		require.EqualValues(t, types.TopicID(1), row["topicId"])
	})
}
*/

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
