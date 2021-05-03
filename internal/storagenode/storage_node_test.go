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
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestStorageNodeRunAndClose(t *testing.T) {
	defer goleak.VerifyNone(t)

	sn, err := New(context.TODO(), WithListenAddress("localhost:0"), WithVolumes(testVolume(t)))
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

func TestStorageNodeGetPrevCommitInfo(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		storageNodeID = types.StorageNodeID(1)
		logStreamID1  = types.LogStreamID(1)
		logStreamID2  = types.LogStreamID(2)
	)

	// create storage node
	sn, err := New(
		context.TODO(),
		WithListenAddress("localhost:0"),
		WithVolumes(testVolume(t), testVolume(t)),
		WithStorageNodeID(storageNodeID),
	)
	require.NoError(t, err)

	var wg sync.WaitGroup
	defer func() {
		sn.Close()
		wg.Wait()
	}()

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
	snmd, err := sn.GetMetadata(context.TODO())
	assert.NoError(t, err)

	// AddLogStream: LSID=1, expected=ok
	// AddLogStream: LSID=2, expected=ok
	assert.Len(t, snmd.GetStorageNode().GetStorages(), 2)

	lsPath, err := sn.AddLogStream(context.TODO(), logStreamID1, snmd.GetStorageNode().GetStorages()[0].GetPath())
	assert.NoError(t, err)
	assert.Positive(t, len(lsPath))

	lsPath, err = sn.AddLogStream(context.TODO(), logStreamID2, snmd.GetStorageNode().GetStorages()[1].GetPath())
	assert.NoError(t, err)
	assert.Positive(t, len(lsPath))

	status, _, err := sn.Seal(context.TODO(), logStreamID1, types.InvalidGLSN)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, status)

	status, _, err = sn.Seal(context.TODO(), logStreamID2, types.InvalidGLSN)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, status)

	assert.NoError(t, sn.Unseal(context.TODO(), logStreamID1))
	assert.NoError(t, sn.Unseal(context.TODO(), logStreamID2))

	snmd, err = sn.GetMetadata(context.TODO())
	assert.NoError(t, err)
	require.Len(t, snmd.GetLogStreams(), 2)
	lsmd, ok := snmd.GetLogStream(logStreamID1)
	require.True(t, ok)
	require.Equal(t, varlogpb.LogStreamStatusRunning, lsmd.GetStatus())
	lsmd, ok = snmd.GetLogStream(logStreamID2)
	require.True(t, ok)
	require.Equal(t, varlogpb.LogStreamStatusRunning, lsmd.GetStatus())

	// Append
	for i := 0; i < 10; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			lsid := logStreamID1
			if i%2 != 0 {
				lsid = logStreamID2
			}
			writer, ok := sn.ReadWriter(lsid)
			require.True(t, ok)
			_, err := writer.Append(context.TODO(), []byte("foo"))
			require.NoError(t, err)
		}()
	}

	require.Eventually(t, func() bool {
		reports, err := sn.lsr.GetReport(context.TODO())
		require.NoError(t, err)
		require.Len(t, reports, 2)
		return reports[0].GetUncommittedLLSNLength() == 5 && reports[1].GetUncommittedLLSNLength() == 5
	}, time.Second, 10*time.Millisecond)

	// LSID | LLSN | GLSN | HWM | PrevHWM
	//    1 |    1 |    5 |  20 |       0
	//    1 |    2 |    6 |  20 |       0
	//    1 |    3 |    7 |  20 |       0
	//    1 |    4 |    8 |  20 |       0
	//    1 |    5 |    9 |  20 |       0
	//    2 |    1 |   11 |  20 |       0
	//    2 |    2 |   12 |  20 |       0
	//    2 |    3 |   13 |  20 |       0
	//    2 |    4 |   14 |  20 |       0
	//    2 |    5 |   15 |  20 |       0
	sn.lsr.Commit(context.TODO(), []*snpb.LogStreamCommitResult{
		{
			LogStreamID:         logStreamID1,
			CommittedLLSNOffset: 1,
			CommittedGLSNOffset: 5,
			CommittedGLSNLength: 5,
			HighWatermark:       20,
			PrevHighWatermark:   0,
		},
		{
			LogStreamID:         logStreamID2,
			CommittedLLSNOffset: 1,
			CommittedGLSNOffset: 11,
			CommittedGLSNLength: 5,
			HighWatermark:       20,
			PrevHighWatermark:   0,
		},
	})

	require.Eventually(t, func() bool {
		reports, err := sn.lsr.GetReport(context.TODO())
		require.NoError(t, err)
		require.Len(t, reports, 2)
		return reports[0].GetHighWatermark() == 20 && reports[1].GetHighWatermark() == 20
	}, time.Second, 10*time.Millisecond)

	infos, err := sn.GetPrevCommitInfo(context.TODO(), 0)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID1,
		Status:              snpb.GetPrevCommitStatusOK,
		CommittedLLSNOffset: 1,
		CommittedGLSNOffset: 5,
		CommittedGLSNLength: 5,
		HighestWrittenLLSN:  5,
		HighWatermark:       20,
		PrevHighWatermark:   0,
	})
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID2,
		Status:              snpb.GetPrevCommitStatusOK,
		CommittedLLSNOffset: 1,
		CommittedGLSNOffset: 11,
		CommittedGLSNLength: 5,
		HighestWrittenLLSN:  5,
		HighWatermark:       20,
		PrevHighWatermark:   0,
	})

	infos, err = sn.GetPrevCommitInfo(context.TODO(), 1)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID1,
		Status:              snpb.GetPrevCommitStatusInconsistent,
		CommittedLLSNOffset: 0,
		CommittedGLSNOffset: 0,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  5,
		HighWatermark:       0,
		PrevHighWatermark:   0,
	})
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID2,
		Status:              snpb.GetPrevCommitStatusInconsistent,
		CommittedLLSNOffset: 0,
		CommittedGLSNOffset: 0,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  5,
		HighWatermark:       0,
		PrevHighWatermark:   0,
	})

	infos, err = sn.GetPrevCommitInfo(context.TODO(), 5)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID1,
		Status:              snpb.GetPrevCommitStatusInconsistent,
		CommittedLLSNOffset: 0,
		CommittedGLSNOffset: 0,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  5,
		HighWatermark:       0,
		PrevHighWatermark:   0,
	})
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID2,
		Status:              snpb.GetPrevCommitStatusInconsistent,
		CommittedLLSNOffset: 0,
		CommittedGLSNOffset: 0,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  5,
		HighWatermark:       0,
		PrevHighWatermark:   0,
	})

	infos, err = sn.GetPrevCommitInfo(context.TODO(), 20)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID1,
		Status:              snpb.GetPrevCommitStatusNotFound,
		CommittedLLSNOffset: 0,
		CommittedGLSNOffset: 0,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  5,
		HighWatermark:       0,
		PrevHighWatermark:   0,
	})
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID2,
		Status:              snpb.GetPrevCommitStatusNotFound,
		CommittedLLSNOffset: 0,
		CommittedGLSNOffset: 0,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  5,
		HighWatermark:       0,
		PrevHighWatermark:   0,
	})

	infos, err = sn.GetPrevCommitInfo(context.TODO(), 21)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID1,
		Status:              snpb.GetPrevCommitStatusNotFound,
		CommittedLLSNOffset: 0,
		CommittedGLSNOffset: 0,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  5,
		HighWatermark:       0,
		PrevHighWatermark:   0,
	})
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID2,
		Status:              snpb.GetPrevCommitStatusNotFound,
		CommittedLLSNOffset: 0,
		CommittedGLSNOffset: 0,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  5,
		HighWatermark:       0,
		PrevHighWatermark:   0,
	})
}
