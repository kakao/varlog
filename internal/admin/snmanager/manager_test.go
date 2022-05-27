package snmanager

import (
	"context"
	"errors"
	"testing"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.daumkakao.com/varlog/varlog/internal/admin/mrmanager"
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestStorageNodeManager_InvalidOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)

	_, err := New(context.Background())
	assert.Error(t, err)

	_, err = New(context.Background(),
		WithClusterMetadataView(cmView),
		WithLogger(nil),
	)
	assert.Error(t, err)
}

func TestStorageNodeManager_RegisterUnregister(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil).AnyTimes()

	snmgr, err := New(context.Background(),
		WithClusterID(1),
		WithClusterMetadataView(cmView),
	)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, snmgr.Close())
	}()

	ts := storagenode.TestNewRPCServer(t, ctrl, 1)
	ts.Run()
	defer ts.Close()

	snmgr.AddStorageNode(context.Background(), ts.StorageNodeID(), ts.Address())
	snmgr.AddStorageNode(context.Background(), ts.StorageNodeID(), ts.Address()) // idempotent
	assert.True(t, snmgr.Contains(ts.StorageNodeID()))
	assert.True(t, snmgr.ContainsAddress(ts.Address()))

	snmgr.RemoveStorageNode(ts.StorageNodeID())
	snmgr.RemoveStorageNode(ts.StorageNodeID()) // idempotent
	assert.False(t, snmgr.Contains(ts.StorageNodeID()))
	assert.False(t, snmgr.ContainsAddress(ts.Address()))

	// Register unreachable storage node.
	snmgr.AddStorageNode(context.Background(), ts.StorageNodeID()+1, "unreachable")
	assert.True(t, snmgr.Contains(ts.StorageNodeID()+1))
	assert.True(t, snmgr.ContainsAddress("unreachable"))
	_, err = snmgr.GetMetadata(context.Background(), ts.StorageNodeID()+1)
	assert.Error(t, err)

	// Remove not existing storage node.
	snmgr.RemoveStorageNode(ts.StorageNodeID() + 1)
}

func TestStorageNodeManager_RegisterStorageNodeEventually(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil)

	snmgr, err := New(context.Background(),
		WithClusterID(1),
		WithClusterMetadataView(cmView),
	)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, snmgr.Close())
	}()

	ts := storagenode.TestNewRPCServer(t, ctrl, 1)
	ts.Run()
	defer ts.Close()

	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		StorageNodes: []*varlogpb.StorageNodeDescriptor{
			{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: ts.StorageNodeID(),
					Address:       ts.Address(),
				},
			},
		},
	}, nil)

	_, err = snmgr.GetMetadata(context.Background(), ts.StorageNodeID())
	assert.Error(t, err)
	assert.True(t, snmgr.Contains(ts.StorageNodeID()))
	assert.True(t, snmgr.ContainsAddress(ts.Address()))
}

func TestStorageNodeManager_AddLogStreamReplica(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil).AnyTimes()

	snmgr, err := New(context.Background(),
		WithClusterID(1),
		WithClusterMetadataView(cmView),
	)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, snmgr.Close())
	}()

	ts := storagenode.TestNewRPCServer(t, ctrl, 1)
	ts.Run()
	defer ts.Close()
	ts.MockManagementServer.EXPECT().AddLogStreamReplica(gomock.Any(), gomock.Any()).Return(
		&snpb.AddLogStreamReplicaResponse{}, nil,
	)

	// Not registered yet.
	err = snmgr.AddLogStreamReplica(context.Background(), ts.StorageNodeID(), 1, 1, "/tmp")
	assert.Error(t, err)

	snmgr.AddStorageNode(context.Background(), ts.StorageNodeID(), ts.Address())
	err = snmgr.AddLogStreamReplica(context.Background(), ts.StorageNodeID(), 1, 1, "/tmp")
	assert.NoError(t, err)
}

func TestStorageNodeManager_AddLogStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil).AnyTimes()

	snmgr, err := New(context.Background(),
		WithClusterID(1),
		WithClusterMetadataView(cmView),
	)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, snmgr.Close())
	}()

	ts1 := storagenode.TestNewRPCServer(t, ctrl, 1)
	ts1.Run()
	defer ts1.Close()

	ts2 := storagenode.TestNewRPCServer(t, ctrl, 2)
	ts2.Run()
	defer ts2.Close()

	// One of the storage nodes is not registered.
	snmgr.AddStorageNode(context.Background(), ts1.StorageNodeID(), ts1.Address())
	ts1.MockManagementServer.EXPECT().AddLogStreamReplica(gomock.Any(), gomock.Any()).Return(
		&snpb.AddLogStreamReplicaResponse{}, nil,
	).AnyTimes()
	err = snmgr.AddLogStream(context.Background(), &varlogpb.LogStreamDescriptor{
		TopicID:     1,
		LogStreamID: 1,
		Replicas: []*varlogpb.ReplicaDescriptor{
			{
				StorageNodeID: ts1.StorageNodeID(),
				Path:          "/tmp",
			},
			{
				StorageNodeID: ts2.StorageNodeID(),
				Path:          "/tmp",
			},
		},
	})
	assert.Error(t, err)

	// One of the storage nodes returns an error.
	snmgr.AddStorageNode(context.Background(), ts2.StorageNodeID(), ts2.Address())
	ts2.MockManagementServer.EXPECT().AddLogStreamReplica(gomock.Any(), gomock.Any()).Return(
		nil, errors.New("error"),
	).Times(1)
	err = snmgr.AddLogStream(context.Background(), &varlogpb.LogStreamDescriptor{
		TopicID:     1,
		LogStreamID: 1,
		Replicas: []*varlogpb.ReplicaDescriptor{
			{
				StorageNodeID: ts1.StorageNodeID(),
				Path:          "/tmp",
			},
			{
				StorageNodeID: ts2.StorageNodeID(),
				Path:          "/tmp",
			},
		},
	})
	assert.Error(t, err)

	// Both succeed.
	ts2.MockManagementServer.EXPECT().AddLogStreamReplica(gomock.Any(), gomock.Any()).Return(
		&snpb.AddLogStreamReplicaResponse{}, nil,
	).AnyTimes()
	err = snmgr.AddLogStream(context.Background(), &varlogpb.LogStreamDescriptor{
		TopicID:     1,
		LogStreamID: 1,
		Replicas: []*varlogpb.ReplicaDescriptor{
			{
				StorageNodeID: ts1.StorageNodeID(),
				Path:          "/tmp",
			},
			{
				StorageNodeID: ts2.StorageNodeID(),
				Path:          "/tmp",
			},
		},
	})
	assert.NoError(t, err)
}

func TestStorageNodeManager_Seal(t *testing.T) {
	const (
		tpid     = types.TopicID(1)
		lsid     = types.LogStreamID(1)
		lastGLSN = types.GLSN(10)
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil)

	snmgr, err := New(context.Background(),
		WithClusterID(1),
		WithClusterMetadataView(cmView),
	)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, snmgr.Close())
	}()

	// It could not fetch cluster metadata.
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, errors.New("error"))
	_, err = snmgr.Seal(context.Background(), tpid, lsid, lastGLSN)
	assert.Error(t, err)

	ts1 := storagenode.TestNewRPCServer(t, ctrl, 1)
	ts1.Run()
	defer ts1.Close()

	ts2 := storagenode.TestNewRPCServer(t, ctrl, 2)
	ts2.Run()
	defer ts2.Close()

	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{
				TopicID:     tpid,
				LogStreamID: lsid,
				Replicas: []*varlogpb.ReplicaDescriptor{
					{
						StorageNodeID: ts1.StorageNodeID(),
						Path:          "/tmp",
					},
					{
						StorageNodeID: ts2.StorageNodeID(),
						Path:          "/tmp",
					},
				},
			},
		},
	}, nil).AnyTimes()

	// One of the storage nodes is not registered.
	snmgr.AddStorageNode(context.Background(), ts1.StorageNodeID(), ts1.Address())
	ts1.MockManagementServer.EXPECT().Seal(gomock.Any(), gomock.Any()).Return(
		&snpb.SealResponse{}, nil,
	).AnyTimes()
	_, err = snmgr.Seal(context.Background(), tpid, lsid, lastGLSN)
	assert.Error(t, err)

	// One of the storage nodes returns an error.
	snmgr.AddStorageNode(context.Background(), ts2.StorageNodeID(), ts2.Address())
	ts2.MockManagementServer.EXPECT().Seal(gomock.Any(), gomock.Any()).Return(
		nil, errors.New("error"),
	).Times(1)
	_, err = snmgr.Seal(context.Background(), tpid, lsid, lastGLSN)
	// TODO (jun): Check this behavior.
	assert.NoError(t, err)

	// Both succeed.
	ts2.MockManagementServer.EXPECT().Seal(gomock.Any(), gomock.Any()).Return(
		&snpb.SealResponse{}, nil,
	).AnyTimes()
	_, err = snmgr.Seal(context.Background(), tpid, lsid, lastGLSN)
	assert.NoError(t, err)
}

func TestStorageNodeManager_Unseal(t *testing.T) {
	const (
		tpid = types.TopicID(1)
		lsid = types.LogStreamID(1)
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil)

	snmgr, err := New(context.Background(),
		WithClusterID(1),
		WithClusterMetadataView(cmView),
	)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, snmgr.Close())
	}()

	// It could not fetch cluster metadata.
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, errors.New("error"))
	err = snmgr.Unseal(context.Background(), tpid, lsid)
	assert.Error(t, err)

	ts1 := storagenode.TestNewRPCServer(t, ctrl, 1)
	ts1.Run()
	defer ts1.Close()

	ts2 := storagenode.TestNewRPCServer(t, ctrl, 2)
	ts2.Run()
	defer ts2.Close()

	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{
				TopicID:     tpid,
				LogStreamID: lsid,
				Replicas: []*varlogpb.ReplicaDescriptor{
					{
						StorageNodeID: ts1.StorageNodeID(),
						Path:          "/tmp",
					},
					{
						StorageNodeID: ts2.StorageNodeID(),
						Path:          "/tmp",
					},
				},
			},
		},
	}, nil).AnyTimes()

	// One of the storage nodes is not registered.
	snmgr.AddStorageNode(context.Background(), ts1.StorageNodeID(), ts1.Address())
	ts1.MockManagementServer.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(
		&pbtypes.Empty{}, nil,
	).AnyTimes()
	err = snmgr.Unseal(context.Background(), tpid, lsid)
	assert.Error(t, err)

	// One of the storage nodes returns an error.
	snmgr.AddStorageNode(context.Background(), ts2.StorageNodeID(), ts2.Address())
	ts2.MockManagementServer.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(
		nil, errors.New("error"),
	).Times(1)
	err = snmgr.Unseal(context.Background(), tpid, lsid)
	assert.Error(t, err)

	// Both succeed.
	ts2.MockManagementServer.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(
		&pbtypes.Empty{}, nil,
	).AnyTimes()
	err = snmgr.Unseal(context.Background(), tpid, lsid)
	assert.NoError(t, err)
}

func TestStorageNodeManager_Trim(t *testing.T) {
	const (
		tpid     = types.TopicID(1)
		lsid1    = types.LogStreamID(1)
		lsid2    = types.LogStreamID(2)
		lastGLSN = types.GLSN(10)
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil)

	snmgr, err := New(context.Background(),
		WithClusterID(1),
		WithClusterMetadataView(cmView),
	)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, snmgr.Close())
	}()

	// It could not fetch cluster metadata.
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, errors.New("error"))
	_, err = snmgr.Trim(context.Background(), tpid, lastGLSN)
	assert.Error(t, err)

	// No such topic.
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil)
	_, err = snmgr.Trim(context.Background(), tpid, lastGLSN)
	assert.Error(t, err)

	ts1 := storagenode.TestNewRPCServer(t, ctrl, 1)
	ts1.Run()
	defer ts1.Close()

	ts2 := storagenode.TestNewRPCServer(t, ctrl, 2)
	ts2.Run()
	defer ts2.Close()

	// Inconsistency between topic descriptor and log stream descriptors.
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{
				TopicID:     tpid,
				LogStreamID: lsid1,
				Replicas: []*varlogpb.ReplicaDescriptor{
					{
						StorageNodeID: ts1.StorageNodeID(),
						Path:          "/tmp",
					},
					{
						StorageNodeID: ts2.StorageNodeID(),
						Path:          "/tmp",
					},
				},
			},
		},
		Topics: []*varlogpb.TopicDescriptor{
			{
				TopicID:    tpid,
				LogStreams: []types.LogStreamID{lsid1, lsid2},
			},
		},
	}, nil).MaxTimes(3)
	_, err = snmgr.Trim(context.Background(), tpid, lastGLSN)
	assert.Error(t, err)

	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{
				TopicID:     tpid,
				LogStreamID: lsid1,
				Replicas: []*varlogpb.ReplicaDescriptor{
					{
						StorageNodeID: ts1.StorageNodeID(),
						Path:          "/tmp",
					},
					{
						StorageNodeID: ts2.StorageNodeID(),
						Path:          "/tmp",
					},
				},
			},
			{
				TopicID:     tpid,
				LogStreamID: lsid2,
				Replicas: []*varlogpb.ReplicaDescriptor{
					{
						StorageNodeID: ts1.StorageNodeID(),
						Path:          "/tmp",
					},
					{
						StorageNodeID: ts2.StorageNodeID(),
						Path:          "/tmp",
					},
				},
			},
		},
		Topics: []*varlogpb.TopicDescriptor{
			{
				TopicID:    tpid,
				LogStreams: []types.LogStreamID{lsid1, lsid2},
			},
		},
	}, nil).AnyTimes()

	// One of the storage nodes is not registered.
	snmgr.AddStorageNode(context.Background(), ts1.StorageNodeID(), ts1.Address())
	ts1.MockManagementServer.EXPECT().Trim(gomock.Any(), gomock.Any()).Return(
		&snpb.TrimResponse{}, nil,
	).AnyTimes()
	_, err = snmgr.Trim(context.Background(), tpid, lastGLSN)
	assert.Error(t, err)

	// One of the storage nodes returns an error.
	snmgr.AddStorageNode(context.Background(), ts2.StorageNodeID(), ts2.Address())
	ts2.MockManagementServer.EXPECT().Trim(gomock.Any(), gomock.Any()).Return(
		nil, errors.New("error"),
	).Times(1)
	_, err = snmgr.Trim(context.Background(), tpid, lastGLSN)
	assert.Error(t, err)

	// Both succeed.
	ts2.MockManagementServer.EXPECT().Trim(gomock.Any(), gomock.Any()).Return(
		&snpb.TrimResponse{}, nil,
	).Times(1)
	_, err = snmgr.Trim(context.Background(), tpid, lastGLSN)
	assert.NoError(t, err)

	// Partial failure/success.
	ts2.MockManagementServer.EXPECT().Trim(gomock.Any(), gomock.Any()).Return(
		&snpb.TrimResponse{
			Results: map[types.LogStreamID]string{
				lsid2: "error",
			},
		}, nil,
	).Times(1)
	res, err := snmgr.Trim(context.Background(), tpid, lastGLSN)
	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, lsid2, res[0].LogStreamID)
	assert.Equal(t, "error", res[0].Error)
}

func TestStorageNodeManager_Sync(t *testing.T) {
	const (
		tpid     = types.TopicID(1)
		lsid     = types.LogStreamID(1)
		snid1    = types.StorageNodeID(1)
		snid2    = types.StorageNodeID(2)
		lastGLSN = types.GLSN(10)
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil)

	snmgr, err := New(context.Background(),
		WithClusterID(1),
		WithClusterMetadataView(cmView),
	)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, snmgr.Close())
	}()

	// It could not fetch cluster metadata.
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, errors.New("error"))
	_, err = snmgr.Sync(context.Background(), tpid, lsid, snid1, snid2, lastGLSN)
	assert.Error(t, err)

	// No such storage node.
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{
				TopicID:     tpid,
				LogStreamID: lsid,
				Replicas: []*varlogpb.ReplicaDescriptor{
					{
						StorageNodeID: snid1,
						Path:          "/tmp",
					},
				},
			},
		},
	}, nil)
	_, err = snmgr.Sync(context.Background(), tpid, lsid, snid1, snid2, lastGLSN)
	assert.Error(t, err)

	ts1 := storagenode.TestNewRPCServer(t, ctrl, snid1)
	ts1.Run()
	defer ts1.Close()

	ts2 := storagenode.TestNewRPCServer(t, ctrl, snid2)
	ts2.Run()
	defer ts2.Close()

	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{
				TopicID:     tpid,
				LogStreamID: lsid,
				Replicas: []*varlogpb.ReplicaDescriptor{
					{
						StorageNodeID: ts1.StorageNodeID(),
						Path:          "/tmp",
					},
					{
						StorageNodeID: ts2.StorageNodeID(),
						Path:          "/tmp",
					},
				},
			},
		},
	}, nil).AnyTimes()

	// Nothing is registered.
	_, err = snmgr.Sync(context.Background(), tpid, lsid, snid1, snid2, lastGLSN)
	assert.Error(t, err)

	// One of the storage nodes is not registered.
	snmgr.AddStorageNode(context.Background(), ts1.StorageNodeID(), ts1.Address())
	ts1.MockManagementServer.EXPECT().Sync(gomock.Any(), gomock.Any()).Return(
		&snpb.SyncResponse{}, nil,
	).AnyTimes()
	_, err = snmgr.Sync(context.Background(), tpid, lsid, snid1, snid2, lastGLSN)
	assert.Error(t, err)

	// Both are registered.
	snmgr.AddStorageNode(context.Background(), ts2.StorageNodeID(), ts2.Address())
	_, err = snmgr.Sync(context.Background(), tpid, lsid, snid1, snid2, lastGLSN)
	assert.NoError(t, err)
}
