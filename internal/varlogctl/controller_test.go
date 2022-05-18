package varlogctl_test

import (
	"context"
	"encoding/json"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/internal/varlogctl"
	"github.daumkakao.com/varlog/varlog/internal/varlogctl/logstream"
	"github.daumkakao.com/varlog/varlog/internal/varlogctl/metarepos"
	"github.daumkakao.com/varlog/varlog/internal/varlogctl/result"
	"github.daumkakao.com/varlog/varlog/internal/varlogctl/storagenode"
	"github.daumkakao.com/varlog/varlog/internal/varlogctl/topic"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
)

func testController(t *testing.T, admin varlog.Admin, executeFunc varlogctl.ExecuteFunc, resultCheck func(result result.Result)) {
	vc, err := varlogctl.New(
		varlogctl.WithExecuteFunc(executeFunc),
		varlogctl.WithAdmin(admin),
	)
	require.NoError(t, err)

	res := vc.Execute(context.Background())

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

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestMetadataRepository(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	admin := varlog.NewMockAdmin(ctrl)

	// Describe
	admin.EXPECT().GetMRMembers(gomock.Any()).Return(&vmspb.GetMRMembersResponse{}, nil)
	testController(t, admin, metarepos.Describe(), func(res result.Result) {
		require.NoError(t, res.Err())
		require.Equal(t, 0, res.NumberOfDataItem())
	})

	// Describe
	admin.EXPECT().GetMRMembers(gomock.Any()).Return(&vmspb.GetMRMembersResponse{
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

	admin.EXPECT().GetStorageNodes(gomock.Any()).Return(map[types.StorageNodeID]*snpb.StorageNodeMetadataDescriptor{
		types.StorageNodeID(1): {
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(1),
				Address:       "sn1",
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
					Path: "/tmp/data1",
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
	admin.EXPECT().Topics(gomock.Any()).Return(nil, nil)
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
	admin.EXPECT().Topics(gomock.Any()).Return([]varlogpb.TopicDescriptor{td}, nil).Times(2)
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
	admin.EXPECT().UnregisterTopic(gomock.Any(), gomock.Any()).Return(&vmspb.UnregisterTopicResponse{}, nil)
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
					Path:          "/tmp/data1",
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
