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
	err = sn.RemoveLogStream(context.TODO(), topicID, logStreamID)
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
	assert.Error(t, sn.Unseal(context.Background(), tpID+1, lsID, []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
			TopicID:     tpID,
			LogStreamID: lsID,
		},
	}))

	// Unseal: OK
	assert.NoError(t, sn.Unseal(context.Background(), tpID, lsID, []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
			TopicID:     tpID,
			LogStreamID: lsID,
		},
	}))
}

func TestStorageNodeGetPrevCommitInfo(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		storageNodeID = types.StorageNodeID(1)
		topicID       = types.TopicID(1)
		logStreamID1  = types.LogStreamID(1)
		logStreamID2  = types.LogStreamID(2)
	)

	tsn := newTestStorageNode(t, storageNodeID, 2)
	defer tsn.close()
	sn := tsn.sn

	var wg sync.WaitGroup

	// AddLogStream: LSID=1, expected=ok
	// AddLogStream: LSID=2, expected=ok
	snmd, err := sn.GetMetadata(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, snmd.GetStorageNode().GetStorages(), 2)

	lsPath, err := sn.AddLogStream(context.TODO(), topicID, logStreamID1, snmd.GetStorageNode().GetStorages()[0].GetPath())
	assert.NoError(t, err)
	assert.Positive(t, len(lsPath))

	lsPath, err = sn.AddLogStream(context.TODO(), topicID, logStreamID2, snmd.GetStorageNode().GetStorages()[1].GetPath())
	assert.NoError(t, err)
	assert.Positive(t, len(lsPath))

	status, _, err := sn.Seal(context.TODO(), topicID, logStreamID1, types.InvalidGLSN)
	require.NoError(t, err)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, status)

	status, _, err = sn.Seal(context.TODO(), topicID, logStreamID2, types.InvalidGLSN)
	require.NoError(t, err)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, status)

	assert.NoError(t, sn.Unseal(context.TODO(), topicID, logStreamID1, []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: storageNodeID,
			},
			LogStreamID: logStreamID1,
		},
	}))
	assert.NoError(t, sn.Unseal(context.TODO(), topicID, logStreamID2, []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: storageNodeID,
			},
			LogStreamID: logStreamID2,
		},
	}))

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
			writer, ok := sn.ReadWriter(topicID, lsid)
			require.True(t, ok)
			_, err := writer.Append(context.TODO(), []byte("foo"))
			require.NoError(t, err)
		}()
	}

	require.Eventually(t, func() bool {
		rsp := snpb.GetReportResponse{}
		err := sn.lsr.GetReport(context.TODO(), &rsp)
		require.NoError(t, err)

		reports := rsp.UncommitReports
		require.Len(t, reports, 2)
		return reports[0].GetUncommittedLLSNLength() == 5 && reports[1].GetUncommittedLLSNLength() == 5
	}, time.Second, 10*time.Millisecond)

	// LSID | LLSN | GLSN | Ver
	//    1 |    1 |    5 |   2
	//    1 |    2 |    6 |   2
	//    1 |    3 |    7 |   2
	//    1 |    4 |    8 |   2
	//    1 |    5 |    9 |   2
	//    2 |    1 |   11 |   2
	//    2 |    2 |   12 |   2
	//    2 |    3 |   13 |   2
	//    2 |    4 |   14 |   2
	//    2 |    5 |   15 |   2
	sn.lsr.Commit(context.TODO(), snpb.LogStreamCommitResult{
		TopicID:             topicID,
		LogStreamID:         logStreamID1,
		CommittedLLSNOffset: 1,
		CommittedGLSNOffset: 5,
		CommittedGLSNLength: 5,
		Version:             2,
	})

	sn.lsr.Commit(context.TODO(), snpb.LogStreamCommitResult{
		TopicID:             topicID,
		LogStreamID:         logStreamID2,
		CommittedLLSNOffset: 1,
		CommittedGLSNOffset: 11,
		CommittedGLSNLength: 5,
		Version:             2,
	})

	require.Eventually(t, func() bool {
		rsp := snpb.GetReportResponse{}
		err := sn.lsr.GetReport(context.TODO(), &rsp)
		if !assert.NoError(t, err) {
			return false
		}

		reports := rsp.UncommitReports
		return len(reports) == 2 &&
			reports[0].GetVersion() == 2 &&
			reports[1].GetVersion() == 2
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
		Version:             2,
	})
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID2,
		Status:              snpb.GetPrevCommitStatusOK,
		CommittedLLSNOffset: 1,
		CommittedGLSNOffset: 11,
		CommittedGLSNLength: 5,
		HighestWrittenLLSN:  5,
		Version:             2,
	})

	infos, err = sn.GetPrevCommitInfo(context.TODO(), 1)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID1,
		Status:              snpb.GetPrevCommitStatusOK,
		CommittedLLSNOffset: 1,
		CommittedGLSNOffset: 5,
		CommittedGLSNLength: 5,
		HighestWrittenLLSN:  5,
		Version:             2,
	})
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID2,
		Status:              snpb.GetPrevCommitStatusOK,
		CommittedLLSNOffset: 1,
		CommittedGLSNOffset: 11,
		CommittedGLSNLength: 5,
		HighestWrittenLLSN:  5,
		Version:             2,
	})

	infos, err = sn.GetPrevCommitInfo(context.TODO(), 2)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID1,
		Status:              snpb.GetPrevCommitStatusNotFound,
		CommittedLLSNOffset: 0,
		CommittedGLSNOffset: 0,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  5,
		Version:             0,
	})
	require.Contains(t, infos, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID2,
		Status:              snpb.GetPrevCommitStatusNotFound,
		CommittedLLSNOffset: 0,
		CommittedGLSNOffset: 0,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  5,
		Version:             0,
	})
}
