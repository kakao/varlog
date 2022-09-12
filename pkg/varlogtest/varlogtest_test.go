package varlogtest_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlogtest"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestVarlogTest(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		clusterID         = types.ClusterID(1)
		replicationFactor = 3

		numStorageNodes       = 5
		numTopics             = 10
		avgLogStreamsPerTopic = 10
		numLogStreams         = numTopics * avgLogStreamsPerTopic / replicationFactor
		avgLogsPerTopic       = 1000
		minLogsPerTopic       = 100
		numLogs               = numTopics * avgLogsPerTopic
		maxBatchSize          = 5
	)

	var remainedLogs = numLogs

	rng := rand.New(rand.NewSource(time.Now().UnixMilli()))

	vt := varlogtest.New(clusterID, replicationFactor)
	adm := vt.Admin()
	vlg := vt.Log()
	defer func() {
		require.NoError(t, vlg.Close())
		require.NoError(t, adm.Close())
	}()

	var (
		topicIDs           = make([]types.TopicID, 0, numTopics)
		topicLogStreamsMap = make(map[types.TopicID][]types.LogStreamID, numTopics)
		logStreamIDs       = make([]types.LogStreamID, 0, numLogStreams)
		globalHWMs         = make(map[types.TopicID]types.GLSN, numTopics)
		localHWMs          = make(map[types.LogStreamID]types.LLSN, numLogStreams)
	)

	// No topic 1
	_, err := adm.DescribeTopic(context.Background(), types.TopicID(1))
	require.Error(t, err)

	// Add topics
	for i := 0; i < numTopics; i++ {
		topicDesc, err := adm.AddTopic(context.Background())
		require.NoError(t, err)
		require.Equal(t, varlogpb.TopicStatusRunning, topicDesc.Status)
		require.Empty(t, topicDesc.LogStreams)
		require.NotContains(t, topicIDs, topicDesc.TopicID)
		topicIDs = append(topicIDs, topicDesc.TopicID)
		topicLogStreamsMap[topicDesc.TopicID] = topicDesc.LogStreams

		rsp, err := adm.DescribeTopic(context.Background(), topicDesc.TopicID)
		require.NoError(t, err)
		require.Equal(t, *topicDesc, rsp.Topic)
		require.Empty(t, rsp.LogStreams)
	}

	// Append logs, but no log stream
	for _, tpID := range topicIDs {
		res := vlg.Append(context.Background(), tpID, nil)
		require.Error(t, res.Err)
	}

	// Add log streams, but no storage node
	for i := 0; i < numLogStreams; i++ {
		tpID := topicIDs[rng.Intn(numTopics)]
		_, err := adm.AddLogStream(context.Background(), tpID, nil)
		require.Error(t, err)
	}

	// Add storage nodes
	for i := 0; i < numStorageNodes; i++ {
		snid := types.StorageNodeID(i + 1)
		addr := fmt.Sprintf("sn%03d", i+1)
		snMetaDesc, err := adm.AddStorageNode(context.Background(), snid, addr)
		require.NoError(t, err)
		require.Equal(t, clusterID, snMetaDesc.ClusterID)
		require.Empty(t, snMetaDesc.LogStreamReplicas)
		require.Equal(t, varlogpb.StorageNodeStatusRunning, snMetaDesc.Status)
		require.Equal(t, addr, snMetaDesc.StorageNode.Address)
		require.NotEmpty(t, snMetaDesc.Storages)
	}

	// Add log streams
	addLogStream := func(tpID types.TopicID) types.LogStreamID {
		lsDesc, err := adm.AddLogStream(context.Background(), tpID, nil)
		require.NoError(t, err)
		require.Equal(t, tpID, lsDesc.TopicID)
		require.Equal(t, varlogpb.LogStreamStatusRunning, lsDesc.Status)
		require.Len(t, lsDesc.Replicas, replicationFactor)

		snIDSet := set.New(replicationFactor)
		for _, replicaDesc := range lsDesc.Replicas {
			require.NotContains(t, snIDSet, replicaDesc.StorageNodeID)
			require.NotEmpty(t, replicaDesc.StorageNodePath)
			snIDSet.Add(replicaDesc.StorageNodeID)
		}
		require.Len(t, snIDSet, replicationFactor)

		rsp, err := adm.DescribeTopic(context.Background(), tpID)
		require.NoError(t, err)
		require.Contains(t, rsp.Topic.LogStreams, lsDesc.LogStreamID)
		require.Condition(t, func() bool {
			for _, lsID := range rsp.Topic.LogStreams {
				if lsID == lsDesc.LogStreamID {
					return true
				}
			}
			return false
		})

		logStreamIDs = append(logStreamIDs, lsDesc.LogStreamID)
		topicLogStreamsMap[tpID] = append(topicLogStreamsMap[tpID], lsDesc.LogStreamID)
		localHWMs[lsDesc.LogStreamID] = types.InvalidLLSN
		return lsDesc.LogStreamID
	}
	for i := 0; i < numTopics; i++ {
		topicID := topicIDs[i]
		logStreamID := addLogStream(topicID)

		logStreamDesc, err := vlg.LogStreamMetadata(context.Background(), topicID, logStreamID) //nolint:staticcheck
		require.NoError(t, err)
		require.Equal(t, varlogpb.LogEntryMeta{
			TopicID:     topicID,
			LogStreamID: logStreamID,
		}, logStreamDesc.Head) //nolint:staticcheck
		require.Equal(t, varlogpb.LogEntryMeta{
			TopicID:     topicID,
			LogStreamID: logStreamID,
		}, logStreamDesc.Tail)

		lsrmd, err := vlg.LogStreamReplicaMetadata(context.Background(), topicID, logStreamID)
		require.NoError(t, err)
		require.Equal(t, varlogpb.LogEntryMeta{
			TopicID:     topicID,
			LogStreamID: logStreamID,
		}, lsrmd.Head())
		require.Equal(t, varlogpb.LogEntryMeta{
			TopicID:     topicID,
			LogStreamID: logStreamID,
		}, lsrmd.Tail())
	}
	for i := 0; i < numLogStreams-numTopics; i++ {
		tpID := topicIDs[rng.Intn(numTopics)]
		addLogStream(tpID)
	}

	// Append logs
	testAppend := func(tpID types.TopicID, batchSize int, appendFunc func([][]byte) varlog.AppendResult) {
		hwm := globalHWMs[tpID]

		if batchSize < 1 {
			batchSize = rng.Intn(maxBatchSize) + 1
		}
		batchData := make([][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			hwm++
			batchData[i] = []byte(fmt.Sprintf("%d,%d", tpID, hwm))
		}

		res := appendFunc(batchData)
		require.NoError(t, res.Err)
		require.Len(t, res.Metadata, batchSize)
		require.NoError(t, res.Err)

		for _, lem := range res.Metadata {
			require.Equal(t, tpID, lem.TopicID)

			globalHWMs[tpID]++
			require.Equal(t, globalHWMs[tpID], lem.GLSN)

			localHWMs[lem.LogStreamID]++
			require.Equal(t, localHWMs[lem.LogStreamID], lem.LLSN)
		}
		remainedLogs -= batchSize
	}
	appendToLog := func(tpID types.TopicID, lsID types.LogStreamID, batchSize int) {
		testAppend(tpID, batchSize, func(dataBatch [][]byte) varlog.AppendResult {
			return vlg.AppendTo(context.Background(), tpID, lsID, dataBatch)
		})
	}
	appendLog := func(tpID types.TopicID, batchSize int) {
		testAppend(tpID, batchSize, func(dataBatch [][]byte) varlog.AppendResult {
			return vlg.Append(context.Background(), tpID, dataBatch)
		})
	}

	// Test if log entries cannot be appended to sealed log streams.
	for i := 0; i < numTopics; i++ {
		tpID := topicIDs[i]
		for _, lsID := range topicLogStreamsMap[tpID] {
			rsp, err := adm.Seal(context.Background(), tpID, lsID)
			require.NoError(t, err)
			require.Equal(t, types.InvalidGLSN, rsp.SealedGLSN)
			require.Condition(t, func() bool {
				ret := len(rsp.LogStreams) > 0
				for _, lsmd := range rsp.LogStreams {
					ret = ret && assert.True(t, lsmd.Status.Sealed())
				}
				return ret
			})

			res := vlg.AppendTo(context.Background(), tpID, lsID, nil)
			require.ErrorIs(t, res.Err, verrors.ErrSealed)

			lsd, err := adm.Unseal(context.Background(), tpID, lsID)
			require.NoError(t, err)
			require.False(t, lsd.Status.Sealed())
		}
	}

	// Append logs
	for i := 0; i < numTopics; i++ {
		tpID := topicIDs[i]
		for _, lsID := range topicLogStreamsMap[tpID] {
			for j := 0; j < minLogsPerTopic; j++ {
				appendToLog(tpID, lsID, 0)
			}
		}
	}

	for remainedLogs > 0 {
		tpID := topicIDs[rng.Intn(numTopics)]
		appendLog(tpID, 0)
	}

	// Subscribe
	subscribe := func(tpID types.TopicID, begin, end types.GLSN) {
		expectedGLSN := begin
		llsnMap := make(map[types.LogStreamID][]types.LLSN)
		onNext := func(logEntry varlogpb.LogEntry, err error) {
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
				return
			}
			require.Equal(t, expectedGLSN, logEntry.GLSN)
			require.Equal(t, []byte(fmt.Sprintf("%d,%d", tpID, expectedGLSN)), logEntry.Data)
			llsnMap[logEntry.LogStreamID] = append(llsnMap[logEntry.LogStreamID], logEntry.LLSN)
			expectedGLSN++
		}
		closer, err := vlg.Subscribe(context.Background(), tpID, types.MinGLSN, globalHWMs[tpID]+1, onNext)
		require.NoError(t, err)
		closer()
		require.Equal(t, end, expectedGLSN)
		for _, llsnList := range llsnMap {
			prev := llsnList[0]
			for _, llsn := range llsnList[1:] {
				require.Equal(t, prev+1, llsn)
				prev++
			}
		}
	}
	subscribeTo := func(tpID types.TopicID, lsID types.LogStreamID, begin, end types.LLSN) {
		subscriber := vlg.SubscribeTo(context.Background(), tpID, lsID, begin, end)
		defer func() {
			require.NoError(t, subscriber.Close())
		}()
		prevGLSN := types.InvalidGLSN
		expectedLLSN := begin
		for {
			logEntry, err := subscriber.Next()
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
				break
			}
			require.Equal(t, expectedLLSN, logEntry.LLSN)
			require.Greater(t, logEntry.GLSN, prevGLSN)
			expectedLLSN++
			prevGLSN = logEntry.GLSN
		}
		require.Equal(t, end, expectedLLSN)
	}

	// SubscribeTo: InvalidArgument
	for tpID, lsIDs := range topicLogStreamsMap {
		for _, lsID := range lsIDs {
			var sub varlog.Subscriber
			sub = vlg.SubscribeTo(context.Background(), tpID, lsID, types.MinLLSN+1, types.MinLLSN)
			_, err := sub.Next()
			require.Error(t, err)
			require.Error(t, sub.Close())

			sub = vlg.SubscribeTo(context.Background(), tpID, lsID, types.InvalidLLSN, types.MinLLSN)
			_, err = sub.Next()
			require.Error(t, err)
			require.Error(t, sub.Close())
		}
	}

	// Test if log entries cannot be appended to sealed log streams.
	// Test if appended log streams are subscribed.
	for i := 0; i < numTopics; i++ {
		tpID := topicIDs[i]
		for _, lsID := range topicLogStreamsMap[tpID] {
			rsp, err := adm.Seal(context.Background(), tpID, lsID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, rsp.SealedGLSN, types.MinGLSN)
			require.Condition(t, func() bool {
				ret := len(rsp.LogStreams) > 0
				for _, lsmd := range rsp.LogStreams {
					ret = ret && assert.True(t, lsmd.Status.Sealed())
				}
				return ret
			})

			res := vlg.AppendTo(context.Background(), tpID, lsID, nil)
			require.ErrorIs(t, res.Err, verrors.ErrSealed)
		}

		for i := 0; i < 100; i++ {
			res := vlg.Append(context.Background(), tpID, nil)
			require.ErrorIs(t, res.Err, verrors.ErrSealed)
		}

		for _, lsID := range topicLogStreamsMap[tpID] {
			lsDesc, err := vlg.LogStreamMetadata(context.Background(), tpID, lsID) //nolint:staticcheck
			require.NoError(t, err)
			require.GreaterOrEqual(t, lsDesc.Tail.LLSN, lsDesc.Head.LLSN) //nolint:staticcheck
			subscribeTo(tpID, lsID, lsDesc.Head.LLSN, lsDesc.Tail.LLSN+1) //nolint:staticcheck

			lsrmd, err := vlg.LogStreamReplicaMetadata(context.Background(), tpID, lsID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, lsrmd.Tail().LLSN, lsrmd.Head().LLSN)
			subscribeTo(tpID, lsID, lsrmd.Head().LLSN, lsrmd.Tail().LLSN+1)

			lsd, err := adm.Unseal(context.Background(), tpID, lsID)
			require.NoError(t, err)
			require.False(t, lsd.Status.Sealed())
		}
	}

	for i := 0; i < numTopics; i++ {
		tpID := topicIDs[i]
		subscribe(tpID, types.MinGLSN, globalHWMs[tpID]+1)
	}

	// Metadata
	for tpID, lsIDs := range topicLogStreamsMap {
		for _, lsID := range lsIDs {
			lsDesc, err := vlg.LogStreamMetadata(context.Background(), tpID, lsID) //nolint:staticcheck
			require.NoError(t, err)
			require.GreaterOrEqual(t, lsDesc.Tail.LLSN, lsDesc.Head.LLSN) //nolint:staticcheck
			subscribeTo(tpID, lsID, lsDesc.Head.LLSN, lsDesc.Tail.LLSN+1) //nolint:staticcheck

			lsrmd, err := vlg.LogStreamReplicaMetadata(context.Background(), tpID, lsID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, lsrmd.Tail().LLSN, lsrmd.Head().LLSN)
			subscribeTo(tpID, lsID, lsrmd.Head().LLSN, lsrmd.Tail().LLSN+1)
		}
	}

	// Stop subscriber
	tpID := topicIDs[0]
	lsID := topicLogStreamsMap[tpID][0]

	lsDesc, err := vlg.LogStreamMetadata(context.Background(), tpID, lsID) //nolint:staticcheck
	require.NoError(t, err)
	require.Equal(t, types.MinLLSN, lsDesc.Head.LLSN) //nolint:staticcheck
	require.GreaterOrEqual(t, lsDesc.Tail.LLSN, types.LLSN(minLogsPerTopic))

	lsrmd, err := vlg.LogStreamReplicaMetadata(context.Background(), tpID, lsID)
	require.NoError(t, err)
	require.Equal(t, types.MinLLSN, lsrmd.Head().LLSN)
	require.GreaterOrEqual(t, lsrmd.Tail().LLSN, types.LLSN(minLogsPerTopic))

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	sub1 := vlg.SubscribeTo(ctx1, tpID, lsID, types.MinLLSN, lsDesc.Tail.LLSN+1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	sub2 := vlg.SubscribeTo(ctx2, tpID, lsID, types.MinLLSN, lsDesc.Tail.LLSN+4)

	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel3()
	sub3 := vlg.SubscribeTo(ctx3, tpID, lsID, types.MinLLSN, lsDesc.Tail.LLSN+4)

	for llsn := types.MinLLSN; llsn <= lsDesc.Tail.LLSN; llsn++ {
		le, err := sub1.Next()
		require.NoError(t, err)
		require.Equal(t, llsn, le.LLSN)
	}

	// EOF
	_, err = sub1.Next()
	require.ErrorIs(t, err, io.EOF)

	// Close
	require.NoError(t, sub1.Close())

	// Next after closing subscriber
	_, err = sub1.Next()
	require.Error(t, err)

	// Already closed
	require.Error(t, sub1.Close())

	for llsn := types.MinLLSN; llsn <= lsDesc.Tail.LLSN; llsn++ {
		le, err := sub2.Next()
		require.NoError(t, err)
		require.Equal(t, llsn, le.LLSN)
	}

	// append -> subscribe
	appendToLog(tpID, lsID, 1)
	le, err := sub2.Next()
	require.NoError(t, err)
	require.Equal(t, lsDesc.Tail.LLSN+1, le.LLSN)

	var wg sync.WaitGroup

	// append, subscribe
	wg.Add(1)
	go func() {
		defer wg.Done()
		le, err := sub2.Next()
		require.NoError(t, err)
		require.Equal(t, lsDesc.Tail.LLSN+2, le.LLSN)
	}()
	time.Sleep(5 * time.Millisecond)
	appendToLog(tpID, lsID, 1)
	wg.Wait()

	// cancel, subscribe
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := sub2.Next()
		require.Error(t, err)
	}()
	time.Sleep(5 * time.Millisecond)
	cancel2()
	wg.Wait()

	// fatal error (context.Canceled)
	_, err = sub2.Next()
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, sub2.Close(), context.Canceled)

	for llsn := types.MinLLSN; llsn <= lsDesc.Tail.LLSN+2; llsn++ {
		le, err := sub3.Next()
		require.NoError(t, err)
		require.Equal(t, llsn, le.LLSN)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := sub3.Next()
		require.Error(t, err)
	}()
	time.Sleep(5 * time.Millisecond)
	vlg.Close()
	wg.Wait()
	require.Error(t, sub3.Close())
}

func TestVarlogTest_Trim(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		clusterID         = types.ClusterID(1)
		replicationFactor = 3
		numLogStreams     = 2
		numLogs           = 10
	)

	vt := varlogtest.New(clusterID, replicationFactor)
	adm := vt.Admin()
	vlg := vt.Log()
	defer func() {
		require.NoError(t, vlg.Close())
		require.NoError(t, adm.Close())
	}()

	td, err := adm.AddTopic(context.Background())
	assert.NoError(t, err)

	for i := 0; i < replicationFactor; i++ {
		snid := types.StorageNodeID(i + 1)
		addr := fmt.Sprintf("sn-%d", i+1)
		_, err := adm.AddStorageNode(context.Background(), snid, addr)
		assert.NoError(t, err)
	}

	var lsds []*varlogpb.LogStreamDescriptor
	for i := 0; i < numLogStreams; i++ {
		lsd, err := adm.AddLogStream(context.Background(), td.TopicID, nil)
		assert.NoError(t, err)
		lsds = append(lsds, lsd)
	}

	lastGLSN := types.InvalidGLSN
	for i := 0; i < numLogs; i++ {
		lsd := lsds[i%numLogStreams]
		res := vlg.AppendTo(context.Background(), td.TopicID, lsd.LogStreamID, [][]byte{nil})
		assert.NoError(t, res.Err)
		assert.Equal(t, lastGLSN+1, res.Metadata[0].GLSN)
		lastGLSN++
	}

	// LSID: 1  2  1  2  1  2  1  2  1  2
	// LLSN: 1  1  2  2  3  3  4  4  5  5
	// GLSN: 1  2  3  4  5  6  7  9  9 10

	trimGLSN := types.GLSN(3)
	res, err := adm.Trim(context.Background(), td.TopicID, trimGLSN)
	assert.NoError(t, err)
	assert.Len(t, res, numLogStreams)
	for _, lsd := range lsds {
		assert.Contains(t, res, lsd.LogStreamID)
		assert.Len(t, res[lsd.LogStreamID], replicationFactor)
		for _, rd := range lsd.Replicas {
			assert.NoError(t, res[lsd.LogStreamID][rd.StorageNodeID])
		}
	}

	// LSID: 1  2  1  2  1  2  1  2  1  2
	// LLSN: X  X  X  2  3  3  4  4  5  5
	// GLSN: X  X  X  4  5  6  7  9  9 10

	for _, lsd := range lsds {
		subscriber := vlg.SubscribeTo(context.Background(), lsd.TopicID, lsd.LogStreamID, types.MinLLSN, types.LLSN(3))
		_, err := subscriber.Next()
		assert.Error(t, err)
		assert.Error(t, subscriber.Close())
	}

	lsd, err := vlg.LogStreamMetadata(context.Background(), td.TopicID, lsds[0].LogStreamID) //nolint:staticcheck
	assert.NoError(t, err)
	assert.Equal(t, types.LLSN(3), lsd.Head.LLSN) //nolint:staticcheck
	assert.Equal(t, types.GLSN(5), lsd.Head.GLSN) //nolint:staticcheck
	assert.Equal(t, types.LLSN(5), lsd.Tail.LLSN)
	assert.Equal(t, types.GLSN(9), lsd.Tail.GLSN)

	lsrmd, err := vlg.LogStreamReplicaMetadata(context.Background(), td.TopicID, lsds[0].LogStreamID)
	assert.NoError(t, err)
	assert.Equal(t, types.LLSN(3), lsrmd.Head().LLSN)
	assert.Equal(t, types.GLSN(5), lsrmd.Head().GLSN)
	assert.Equal(t, types.LLSN(5), lsrmd.Tail().LLSN)
	assert.Equal(t, types.GLSN(9), lsrmd.Tail().GLSN)

	lsd, err = vlg.LogStreamMetadata(context.Background(), td.TopicID, lsds[1].LogStreamID) //nolint:staticcheck
	assert.NoError(t, err)
	assert.Equal(t, types.LLSN(2), lsd.Head.LLSN) //nolint:staticcheck
	assert.Equal(t, types.GLSN(4), lsd.Head.GLSN) //nolint:staticcheck
	assert.Equal(t, types.LLSN(5), lsd.Tail.LLSN)
	assert.Equal(t, types.GLSN(10), lsd.Tail.GLSN)

	lsrmd, err = vlg.LogStreamReplicaMetadata(context.Background(), td.TopicID, lsds[1].LogStreamID)
	assert.NoError(t, err)
	assert.Equal(t, types.LLSN(2), lsrmd.Head().LLSN)
	assert.Equal(t, types.GLSN(4), lsrmd.Head().GLSN)
	assert.Equal(t, types.LLSN(5), lsrmd.Tail().LLSN)
	assert.Equal(t, types.GLSN(10), lsrmd.Tail().GLSN)

	subscriber := vlg.SubscribeTo(context.Background(), td.TopicID, lsds[0].LogStreamID, types.LLSN(3), types.LLSN(6))
	expectedLLSN := types.LLSN(3)
	for i := 0; i < 3; i++ {
		le, err := subscriber.Next()
		assert.NoError(t, err)
		assert.Equal(t, expectedLLSN, le.LLSN)
		expectedLLSN++
	}
	assert.NoError(t, subscriber.Close())

	subscriber = vlg.SubscribeTo(context.Background(), td.TopicID, lsds[1].LogStreamID, types.LLSN(2), types.LLSN(6))
	expectedLLSN = types.LLSN(2)
	for i := 0; i < 4; i++ {
		le, err := subscriber.Next()
		assert.NoError(t, err)
		assert.Equal(t, expectedLLSN, le.LLSN)
		expectedLLSN++
	}
	assert.NoError(t, subscriber.Close())
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
