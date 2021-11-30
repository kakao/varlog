package varlogtest_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/container/set"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlogtest"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
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

	// Add topics
	for i := 0; i < numTopics; i++ {
		topicDesc, err := adm.AddTopic(context.Background())
		require.NoError(t, err)
		require.Equal(t, varlogpb.TopicStatusRunning, topicDesc.Status)
		require.Empty(t, topicDesc.LogStreams)
		require.NotContains(t, topicIDs, topicDesc.TopicID)
		topicIDs = append(topicIDs, topicDesc.TopicID)
		topicLogStreamsMap[topicDesc.TopicID] = topicDesc.LogStreams
	}

	// Append logs, but no log stream
	for _, tpID := range topicIDs {
		_, err := vlg.Append(context.Background(), tpID, nil)
		require.Error(t, err)
	}

	// Add log streams, but no storage node
	for i := 0; i < numLogStreams; i++ {
		tpID := topicIDs[rng.Intn(numTopics)]
		_, err := adm.AddLogStream(context.Background(), tpID, nil)
		require.Error(t, err)
	}

	// Add storage nodes
	for i := 0; i < numStorageNodes; i++ {
		addr := fmt.Sprintf("sn%03d", i+1)
		snMetaDesc, err := adm.AddStorageNode(context.Background(), addr)
		require.NoError(t, err)
		require.Equal(t, clusterID, snMetaDesc.ClusterID)
		require.Empty(t, snMetaDesc.LogStreams)
		snDesc := snMetaDesc.StorageNode
		require.Equal(t, varlogpb.StorageNodeStatusRunning, snDesc.Status)
		require.Equal(t, addr, snDesc.Address)
		require.NotEmpty(t, snDesc.Storages)
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
			require.NotEmpty(t, replicaDesc.Path)
			snIDSet.Add(replicaDesc.StorageNodeID)
		}
		require.Len(t, snIDSet, replicationFactor)

		logStreamIDs = append(logStreamIDs, lsDesc.LogStreamID)
		topicLogStreamsMap[tpID] = append(topicLogStreamsMap[tpID], lsDesc.LogStreamID)
		localHWMs[lsDesc.LogStreamID] = types.InvalidLLSN
		return lsDesc.LogStreamID
	}
	for i := 0; i < numTopics; i++ {
		topicID := topicIDs[i]
		logStreamID := addLogStream(topicID)
		logStreamDesc, err := vlg.LogStreamMetadata(context.Background(), topicID, logStreamID)
		require.NoError(t, err)
		require.Equal(t, varlogpb.LogEntryMeta{
			TopicID:     topicID,
			LogStreamID: logStreamID,
		}, logStreamDesc.Head)
		require.Equal(t, varlogpb.LogEntryMeta{
			TopicID:     topicID,
			LogStreamID: logStreamID,
		}, logStreamDesc.Tail)
	}
	for i := 0; i < numLogStreams-numTopics; i++ {
		tpID := topicIDs[rng.Intn(numTopics)]
		addLogStream(tpID)
	}

	// Append logs
	testAppend := func(tpID types.TopicID, batchSize int, appendFunc func([][]byte) (varlog.AppendResult, error)) {
		hwm := globalHWMs[tpID]

		if batchSize < 1 {
			batchSize = rng.Intn(maxBatchSize) + 1
		}
		batchData := make([][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			hwm++
			batchData[i] = []byte(fmt.Sprintf("%d,%d", tpID, hwm))
		}

		res, err := appendFunc(batchData)
		require.NoError(t, err)
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
		testAppend(tpID, batchSize, func(dataBatch [][]byte) (varlog.AppendResult, error) {
			return vlg.AppendTo(context.Background(), tpID, lsID, dataBatch)
		})
	}
	appendLog := func(tpID types.TopicID, batchSize int) {
		testAppend(tpID, batchSize, func(dataBatch [][]byte) (varlog.AppendResult, error) {
			return vlg.Append(context.Background(), tpID, dataBatch)
		})
	}
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

	for i := 0; i < numTopics; i++ {
		tpID := topicIDs[i]
		subscribe(tpID, types.MinGLSN, globalHWMs[tpID]+1)
	}

	// Metadata
	for tpID, lsIDs := range topicLogStreamsMap {
		for _, lsID := range lsIDs {
			lsDesc, err := vlg.LogStreamMetadata(context.Background(), tpID, lsID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, lsDesc.Tail.LLSN, lsDesc.Head.LLSN)
			subscribeTo(tpID, lsID, lsDesc.Head.LLSN, lsDesc.Tail.LLSN+1)
		}
	}

	// Stop subscriber
	tpID := topicIDs[0]
	lsID := topicLogStreamsMap[tpID][0]

	lsDesc, err := vlg.LogStreamMetadata(context.Background(), tpID, lsID)
	require.NoError(t, err)
	require.Equal(t, types.MinLLSN, lsDesc.Head.LLSN)
	require.GreaterOrEqual(t, lsDesc.Tail.LLSN, types.LLSN(minLogsPerTopic))

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
