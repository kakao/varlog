package storagenode

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestStorageNodeAddLogStream(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		storageNodeID = types.StorageNodeID(1)
		logStreamID   = types.LogStreamID(1)
	)

	// create storage node
	sn, err := New(
		context.TODO(),
		WithListenAddress("localhost:0"),
		WithVolumes(testVolume(t)),
		WithStorageNodeID(storageNodeID),
	)
	require.NoError(t, err)

	var (
		ok   bool
		wg   sync.WaitGroup
		snmd *varlogpb.StorageNodeMetadataDescriptor
		lsmd varlogpb.LogStreamMetadataDescriptor
	)

	// run storage node
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := sn.Run()
		assert.NoError(t, err)
	}()

	// wait for listening
	assert.Eventually(t, func() bool {
		snmd, err := sn.GetMetadata(context.TODO())
		assert.NoError(t, err)
		return len(snmd.GetStorageNode().GetAddress()) > 0

	}, time.Second, 10*time.Millisecond)
	snmd, err = sn.GetMetadata(context.TODO())
	assert.NoError(t, err)

	// AddLogStream: LSID=1, expected=ok
	assert.Len(t, snmd.GetStorageNode().GetStorages(), 1)
	snPath := snmd.GetStorageNode().GetStorages()[0].GetPath()
	lsPath, err := sn.AddLogStream(context.TODO(), logStreamID, snPath)
	assert.NoError(t, err)
	assert.Positive(t, len(lsPath))

	snmd, err = sn.GetMetadata(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, snmd.GetLogStreams(), 1)
	assert.Equal(t, logStreamID, snmd.GetLogStreams()[0].GetLogStreamID())
	lsmd, ok = snmd.GetLogStream(logStreamID)
	assert.True(t, ok)
	assert.Equal(t, lsPath, lsmd.GetPath())

	// AddLogStream: LSID=1, expected=error
	_, err = sn.AddLogStream(context.TODO(), logStreamID, snPath)
	assert.Error(t, err)

	// FIXME: RemoveLogStream doesn't care about liveness of log stream, so this results in
	// resource leak.
	err = sn.RemoveLogStream(context.TODO(), logStreamID)
	assert.NoError(t, err)

	sn.Close()
	wg.Wait()
}
