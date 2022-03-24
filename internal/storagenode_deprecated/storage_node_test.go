package storagenode_deprecated

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestStorageNodeBadConfig(t *testing.T) {
	defer goleak.VerifyNone(t)

	_, err := New(context.Background(), WithListenAddress("localhost:0"))
	require.Error(t, err)
}

func TestStorageNodeRunAndClose(t *testing.T) {
	defer goleak.VerifyNone(t)

	sn, err := New(context.TODO(), WithListenAddress("localhost:0"), WithVolumes(t.TempDir()))
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = sn.Run()
	}()

	sn.Close()
	wg.Wait()
}

func TestStorageNodeAddLogStream(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		storageNodeID = types.StorageNodeID(1)
		logStreamID   = types.LogStreamID(1)
		topicID       = types.TopicID(1)
	)

	tsn := newTestStorageNode(t, storageNodeID, 1)
	defer tsn.close()
	sn := tsn.sn

	var (
		ok   bool
		snmd *varlogpb.StorageNodeMetadataDescriptor
		lsmd varlogpb.LogStreamMetadataDescriptor
	)

	// AddLogStream: LSID=1, expected=ok
	snmd, err := sn.GetMetadata(context.Background())
	require.NoError(t, err)
	assert.Len(t, snmd.GetStorageNode().GetStorages(), 1)
	snPath := snmd.GetStorageNode().GetStorages()[0].GetPath()
	lsPath, err := sn.AddLogStream(context.TODO(), topicID, logStreamID, snPath)
	assert.NoError(t, err)
	assert.Positive(t, len(lsPath))

	// GetMetadata: Check if the log stream is created.
	snmd, err = sn.GetMetadata(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, snmd.GetLogStreams(), 1)
	assert.Equal(t, logStreamID, snmd.GetLogStreams()[0].GetLogStreamID())
	lsmd, ok = snmd.GetLogStream(logStreamID)
	assert.True(t, ok)
	assert.Equal(t, lsPath, lsmd.GetPath())

	// AddLogStream: LSID=1, expected=error
	_, err = sn.AddLogStream(context.TODO(), topicID, logStreamID, snPath)
	assert.Error(t, err)

	// FIXME: RemoveLogStream doesn't care about liveness of log stream, so this results in
	// resource leak.
	err = sn.removeLogStream(context.TODO(), topicID, logStreamID)
	assert.NoError(t, err)
}

func TestStorageNodeIncorrectTopic(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		snID = types.StorageNodeID(1)
		tpID = types.TopicID(1)
		lsID = types.LogStreamID(1)
	)

	tsn := newTestStorageNode(t, snID, 1)
	defer tsn.close()
	sn := tsn.sn

	// AddLogStream
	snmd, err := sn.GetMetadata(context.Background())
	require.NoError(t, err)
	assert.Len(t, snmd.GetStorageNode().GetStorages(), 1)
	snPath := snmd.GetStorageNode().GetStorages()[0].GetPath()
	lsPath, err := sn.AddLogStream(context.Background(), tpID, lsID, snPath)
	assert.NoError(t, err)
	assert.Positive(t, len(lsPath))

	// Seal: ERROR (incorrect topicID)
	_, _, err = sn.Seal(context.Background(), tpID+1, lsID, types.InvalidGLSN)
	require.Error(t, err)

	// Seal: OK
	status, _, err := sn.Seal(context.Background(), tpID, lsID, types.InvalidGLSN)
	require.NoError(t, err)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, status)

	// Unseal: ERROR (incorrect topicID)
	assert.Error(t, sn.unseal(context.Background(), tpID+1, lsID, []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
			TopicID:     tpID,
			LogStreamID: lsID,
		},
	}))

	// Unseal: OK
	assert.NoError(t, sn.unseal(context.Background(), tpID, lsID, []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
			TopicID:     tpID,
			LogStreamID: lsID,
		},
	}))
}
