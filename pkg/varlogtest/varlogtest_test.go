package varlogtest_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/x/mlsa"
	"github.com/kakao/varlog/pkg/varlogtest"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestVarlogTestAddLogStream(t *testing.T) {
	const (
		cid       = types.MinClusterID
		repfactor = 3
	)

	ctx := context.Background()

	addStorageNodes := func(t *testing.T, adm varlog.Admin) []snpb.StorageNodeMetadataDescriptor {
		snmds := make([]snpb.StorageNodeMetadataDescriptor, repfactor)
		for i := 0; i < repfactor; i++ {
			snid := types.MinStorageNodeID + types.StorageNodeID(i)
			snaddr := "sn" + strconv.Itoa(i+1)
			snm, err := adm.AddStorageNode(ctx, snid, snaddr)
			require.NoError(t, err)
			snmds[i] = snm.StorageNodeMetadataDescriptor
		}
		return snmds
	}

	tcs := []struct {
		testf func(t *testing.T, adm varlog.Admin, tpid types.TopicID)
		name  string
	}{
		{
			name: "Succeed",
			testf: func(t *testing.T, adm varlog.Admin, tpid types.TopicID) {
				_ = addStorageNodes(t, adm)

				_, err := adm.AddLogStream(ctx, tpid, nil)
				require.NoError(t, err)
			},
		},
		{
			name: "CouldNotAddLogStream_NoSuchTopic",
			testf: func(t *testing.T, adm varlog.Admin, tpid types.TopicID) {
				_, err := adm.AddLogStream(ctx, tpid+1, nil)
				require.Error(t, err)
			},
		},
		{
			name: "CouldNotAddLogStream_NotEnoughStorageNodes",
			testf: func(t *testing.T, adm varlog.Admin, tpid types.TopicID) {
				_, err := adm.AddLogStream(ctx, tpid, nil)
				require.Error(t, err)
			},
		},
		{
			name: "CouldNotAddLogStream_DuplicateStorageNodeInReplicas",
			testf: func(t *testing.T, adm varlog.Admin, tpid types.TopicID) {
				snmds := addStorageNodes(t, adm)

				replicas := make([]*varlogpb.ReplicaDescriptor, repfactor)
				for i := range replicas {
					replicas[i] = &varlogpb.ReplicaDescriptor{
						StorageNodeID:   snmds[0].StorageNodeID,
						StorageNodePath: snmds[0].Storages[0].Path,
					}
				}
				_, err := adm.AddLogStream(ctx, tpid, replicas)
				require.Error(t, err)
			},
		},
		{
			name: "CouldNotAddLogStream_UnknownStorageNode",
			testf: func(t *testing.T, adm varlog.Admin, tpid types.TopicID) {
				snmds := addStorageNodes(t, adm)

				replicas := make([]*varlogpb.ReplicaDescriptor, repfactor)
				for i := range replicas {
					replicas[i] = &varlogpb.ReplicaDescriptor{
						StorageNodeID:   snmds[i].StorageNodeID + 10,
						StorageNodePath: snmds[i].Storages[0].Path,
					}
				}
				_, err := adm.AddLogStream(ctx, tpid, replicas)
				require.Error(t, err)
			},
		},
		{
			name: "CouldNotAddLogStream_ReplicasLengthLessThanReplicationFactor",
			testf: func(t *testing.T, adm varlog.Admin, tpid types.TopicID) {
				snmds := addStorageNodes(t, adm)

				replicas := make([]*varlogpb.ReplicaDescriptor, repfactor-1)
				for i := 0; i < repfactor-1; i++ {
					replicas[i] = &varlogpb.ReplicaDescriptor{
						StorageNodeID:   snmds[i].StorageNodeID,
						StorageNodePath: snmds[i].Storages[0].Path,
					}
				}
				_, err := adm.AddLogStream(ctx, tpid, replicas)
				require.Error(t, err)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			vt, err := varlogtest.New(
				varlogtest.WithClusterID(cid),
				varlogtest.WithReplicationFactor(repfactor),
			)
			require.NoError(t, err)

			adm := vt.NewAdminClient()
			t.Cleanup(func() {
				require.NoError(t, adm.Close())
			})

			td, err := adm.AddTopic(ctx)
			require.NoError(t, err)

			tc.testf(t, adm, td.TopicID)
		})
	}
}

func TestVarlotTest_LogStreamAppender(t *testing.T) {
	const (
		cid               = types.ClusterID(1)
		numLogs           = 10
		replicationFactor = 3
	)

	tcs := []struct {
		name  string
		testf func(t *testing.T, vadm varlog.Admin, vcli varlog.Log, tpid types.TopicID, lsid types.LogStreamID)
	}{
		{
			name: "Closed",
			testf: func(t *testing.T, vadm varlog.Admin, vcli varlog.Log, tpid types.TopicID, lsid types.LogStreamID) {
				lsa, err := vcli.NewLogStreamAppender(tpid, lsid)
				require.NoError(t, err)

				lsa.Close()
				err = lsa.AppendBatch([][]byte{[]byte("foo")}, nil)
				require.Equal(t, varlog.ErrClosed, err)
			},
		},
		{
			name: "AppendLogs",
			testf: func(t *testing.T, vadm varlog.Admin, vcli varlog.Log, tpid types.TopicID, lsid types.LogStreamID) {
				lsa, err := vcli.NewLogStreamAppender(tpid, lsid)
				require.NoError(t, err)
				defer lsa.Close()

				cb := func(_ []varlogpb.LogEntryMeta, err error) {
					assert.NoError(t, err)
				}
				for i := 0; i < numLogs; i++ {
					err := lsa.AppendBatch([][]byte{[]byte("foo")}, cb)
					require.NoError(t, err)
				}
			},
		},
		{
			name: "CouldNotAppend_Sealed",
			testf: func(t *testing.T, vadm varlog.Admin, vcli varlog.Log, tpid types.TopicID, lsid types.LogStreamID) {
				_, err := vadm.Seal(context.Background(), tpid, lsid)
				require.NoError(t, err)

				lsa, err := vcli.NewLogStreamAppender(tpid, lsid)
				require.NoError(t, err)
				defer lsa.Close()

				cb := func(_ []varlogpb.LogEntryMeta, err error) {
					assert.ErrorIs(t, err, verrors.ErrSealed)
				}
				err = lsa.AppendBatch([][]byte{[]byte("foo")}, cb)
				require.NoError(t, err)
			},
		},
		{
			name: "CloseInCallback",
			testf: func(t *testing.T, vadm varlog.Admin, vcli varlog.Log, tpid types.TopicID, lsid types.LogStreamID) {
				lsa, err := vcli.NewLogStreamAppender(tpid, lsid)
				require.NoError(t, err)

				var wg sync.WaitGroup
				wg.Add(1)
				err = lsa.AppendBatch([][]byte{[]byte("foo")}, func(lem []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
					lsa.Close()
				})
				require.NoError(t, err)
				wg.Wait()
			},
		},
		{
			name: "DoesNotCloseLogStreamAppender",
			testf: func(t *testing.T, vadm varlog.Admin, vcli varlog.Log, tpid types.TopicID, lsid types.LogStreamID) {
				// Closing the log client will shut down the log stream appender forcefully.
				lsa, err := vcli.NewLogStreamAppender(tpid, lsid)
				require.NoError(t, err)

				cb := func(_ []varlogpb.LogEntryMeta, err error) {
					assert.NoError(t, err)
				}
				for i := 0; i < numLogs; i++ {
					err := lsa.AppendBatch([][]byte{[]byte("foo")}, cb)
					require.NoError(t, err)
				}
			},
		},
		{
			name: "Manager",
			testf: func(t *testing.T, vadm varlog.Admin, vcli varlog.Log, tpid types.TopicID, lsid types.LogStreamID) {
				mgr := mlsa.New(vcli)

				_, err := mgr.Get(tpid+1, lsid)
				require.Error(t, err)

				_, err = mgr.Get(tpid, lsid+1)
				require.Error(t, err)

				lsa1, err := mgr.Get(tpid, lsid)
				require.NoError(t, err)
				lsa2, err := mgr.Get(tpid, lsid)
				require.NoError(t, err)

				var wg sync.WaitGroup
				wg.Add(2)
				cb := func(_ []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
				}
				err = lsa1.AppendBatch([][]byte{[]byte("foo")}, cb)
				require.NoError(t, err)
				err = lsa2.AppendBatch([][]byte{[]byte("foo")}, cb)
				require.NoError(t, err)
				wg.Wait()

				lsa1.Close()
				err = lsa2.AppendBatch([][]byte{[]byte("foo")}, nil)
				require.Equal(t, varlog.ErrClosed, err)

				lsa1, err = mgr.Get(tpid, lsid)
				require.NoError(t, err)
				wg.Add(1)
				err = lsa1.AppendBatch([][]byte{[]byte("foo")}, cb)
				require.NoError(t, err)
				err = lsa2.AppendBatch([][]byte{[]byte("foo")}, nil)
				require.Equal(t, varlog.ErrClosed, err)
				wg.Wait()
				lsa1.Close()
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			vt, err := varlogtest.New(
				varlogtest.WithClusterID(cid),
				varlogtest.WithReplicationFactor(replicationFactor),
			)
			require.NoError(t, err)

			adm := vt.NewAdminClient()
			vlg := vt.NewLogClient()
			defer func() {
				require.NoError(t, vlg.Close())
				require.NoError(t, adm.Close())
			}()

			for i := 0; i < replicationFactor; i++ {
				snid := types.StorageNodeID(i + 1)
				addr := fmt.Sprintf("sn%03d", i+1)
				snMetaDesc, err := adm.AddStorageNode(context.Background(), snid, addr)
				require.NoError(t, err)
				require.Equal(t, cid, snMetaDesc.ClusterID)
				require.Empty(t, snMetaDesc.LogStreamReplicas)
				require.Equal(t, varlogpb.StorageNodeStatusRunning, snMetaDesc.Status)
				require.Equal(t, addr, snMetaDesc.StorageNode.Address)
				require.NotEmpty(t, snMetaDesc.Storages)
			}

			td, err := adm.AddTopic(context.Background())
			require.NoError(t, err)
			require.Equal(t, varlogpb.TopicStatusRunning, td.Status)

			lsd, err := adm.AddLogStream(context.Background(), td.TopicID, nil)
			require.NoError(t, err)
			require.Equal(t, td.TopicID, lsd.TopicID)
			require.Equal(t, varlogpb.LogStreamStatusRunning, lsd.Status)
			require.Len(t, lsd.Replicas, replicationFactor)

			tc.testf(t, adm, vlg, td.TopicID, lsd.LogStreamID)
		})
	}
}

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

	remainedLogs := numLogs

	rng := rand.New(rand.NewSource(time.Now().UnixMilli()))

	vt, err := varlogtest.New(
		varlogtest.WithClusterID(clusterID),
		varlogtest.WithReplicationFactor(replicationFactor),
	)
	require.NoError(t, err)

	adm := vt.NewAdminClient()
	vlg := vt.NewLogClient()
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
	_, err = adm.ListLogStreams(context.Background(), types.TopicID(1))
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

		lsds, err := adm.ListLogStreams(context.Background(), topicDesc.TopicID)
		require.NoError(t, err)
		require.Empty(t, lsds)
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

		lsds, err := adm.ListLogStreams(context.Background(), tpID)
		require.NoError(t, err)
		require.True(t, slices.ContainsFunc(lsds, func(lsd varlogpb.LogStreamDescriptor) bool {
			return lsd.LogStreamID == lsDesc.LogStreamID
		}))

		logStreamIDs = append(logStreamIDs, lsDesc.LogStreamID)
		topicLogStreamsMap[tpID] = append(topicLogStreamsMap[tpID], lsDesc.LogStreamID)
		localHWMs[lsDesc.LogStreamID] = types.InvalidLLSN
		return lsDesc.LogStreamID
	}
	for i := 0; i < numTopics; i++ {
		topicID := topicIDs[i]
		logStreamID := addLogStream(topicID)

		first, last, _, err := vlg.PeekLogStream(context.Background(), topicID, logStreamID)
		require.NoError(t, err)
		require.True(t, first.Invalid())
		require.True(t, last.Invalid())
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
			first, last, _, err := vlg.PeekLogStream(context.Background(), tpID, lsID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, last.LLSN, first.LLSN)
			subscribeTo(tpID, lsID, first.LLSN, last.LLSN+1)

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
			first, last, _, err := vlg.PeekLogStream(context.Background(), tpID, lsID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, last.LLSN, first.LLSN)
			subscribeTo(tpID, lsID, first.LLSN, last.LLSN+1)
		}
	}

	// Stop subscriber
	tpID := topicIDs[0]
	lsID := topicLogStreamsMap[tpID][0]

	first, last, _, err := vlg.PeekLogStream(context.Background(), tpID, lsID)
	require.NoError(t, err)
	require.Equal(t, types.MinLLSN, first.LLSN)
	require.GreaterOrEqual(t, last.LLSN, types.LLSN(minLogsPerTopic))

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	sub1 := vlg.SubscribeTo(ctx1, tpID, lsID, types.MinLLSN, last.LLSN+1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	sub2 := vlg.SubscribeTo(ctx2, tpID, lsID, types.MinLLSN, last.LLSN+4)

	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel3()
	sub3 := vlg.SubscribeTo(ctx3, tpID, lsID, types.MinLLSN, last.LLSN+4)

	for llsn := types.MinLLSN; llsn <= last.LLSN; llsn++ {
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

	for llsn := types.MinLLSN; llsn <= last.LLSN; llsn++ {
		le, err := sub2.Next()
		require.NoError(t, err)
		require.Equal(t, llsn, le.LLSN)
	}

	// append -> subscribe
	appendToLog(tpID, lsID, 1)
	le, err := sub2.Next()
	require.NoError(t, err)
	require.Equal(t, last.LLSN+1, le.LLSN)

	var wg sync.WaitGroup

	// append, subscribe
	wg.Add(1)
	go func() {
		defer wg.Done()
		le, err := sub2.Next()
		require.NoError(t, err)
		require.Equal(t, last.LLSN+2, le.LLSN)
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

	for llsn := types.MinLLSN; llsn <= last.LLSN+2; llsn++ {
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

func TestVarlogTest_PeekLogStream(t *testing.T) {
	const (
		clusterID         = types.ClusterID(1)
		replicationFactor = 3

		numLogStreams = 2
		filledLSIdx   = 0
		emptyLSIdx    = 1

		numLogs = 10
	)

	ctx := context.Background()

	tcs := []struct {
		testf func(t *testing.T, adm varlog.Admin, vlg varlog.Log, lsds []*varlogpb.LogStreamDescriptor)
		name  string
	}{
		{
			name: "NewLogStream",
			testf: func(t *testing.T, _ varlog.Admin, vlg varlog.Log, lsds []*varlogpb.LogStreamDescriptor) {
				lsd := lsds[emptyLSIdx]
				first, last, _, err := vlg.PeekLogStream(ctx, lsd.TopicID, lsd.LogStreamID)
				require.NoError(t, err)
				require.True(t, first.GLSN.Invalid())
				require.True(t, last.GLSN.Invalid())
			},
		},
		{
			name: "FilledLogStream",
			testf: func(t *testing.T, _ varlog.Admin, vlg varlog.Log, lsds []*varlogpb.LogStreamDescriptor) {
				lsd := lsds[filledLSIdx]
				first, last, _, err := vlg.PeekLogStream(ctx, lsd.TopicID, lsd.LogStreamID)
				require.NoError(t, err)
				require.Equal(t, types.MinGLSN, first.GLSN)
				require.Equal(t, types.GLSN(numLogs), last.GLSN)
			},
		},
		{
			name: "TrimmedLogStream",
			testf: func(t *testing.T, adm varlog.Admin, vlg varlog.Log, lsds []*varlogpb.LogStreamDescriptor) {
				const trimGLSN = types.GLSN(5)

				res, err := adm.Trim(ctx, lsds[0].TopicID, trimGLSN)
				require.NoError(t, err)
				require.Len(t, res, numLogStreams)
				for _, m := range res {
					require.NotEmpty(t, m)
					for _, err := range m {
						require.NoError(t, err)
					}
				}

				lsd := lsds[filledLSIdx]
				first, last, _, err := vlg.PeekLogStream(ctx, lsd.TopicID, lsd.LogStreamID)
				require.NoError(t, err)
				require.Equal(t, trimGLSN+1, first.GLSN)
				require.Equal(t, types.GLSN(numLogs), last.GLSN)
			},
		},
		{
			name: "FullyTrimmedLogStream",
			testf: func(t *testing.T, adm varlog.Admin, vlg varlog.Log, lsds []*varlogpb.LogStreamDescriptor) {
				const trimGLSN = types.GLSN(numLogs)

				res, err := adm.Trim(ctx, lsds[0].TopicID, trimGLSN)
				require.NoError(t, err)
				require.Len(t, res, numLogStreams)
				for _, m := range res {
					require.NotEmpty(t, m)
					for _, err := range m {
						require.NoError(t, err)
					}
				}

				lsd := lsds[filledLSIdx]
				first, last, _, err := vlg.PeekLogStream(ctx, lsd.TopicID, lsd.LogStreamID)
				require.NoError(t, err)
				require.True(t, first.GLSN.Invalid())
				require.True(t, last.GLSN.Invalid())
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			vt, err := varlogtest.New(
				varlogtest.WithClusterID(clusterID),
				varlogtest.WithReplicationFactor(replicationFactor),
			)
			require.NoError(t, err)

			adm := vt.NewAdminClient()
			vlg := vt.NewLogClient()
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

			lsd := lsds[filledLSIdx]
			for i := 0; i < numLogs; i++ {
				res := vlg.AppendTo(ctx, lsd.TopicID, lsd.LogStreamID, [][]byte{nil})
				require.NoError(t, res.Err)
				assert.Equal(t, types.GLSN(i+1), res.Metadata[0].GLSN)
			}

			tc.testf(t, adm, vlg, lsds)
		})
	}
}

func TestVarlogTest_Trim(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		clusterID         = types.ClusterID(1)
		replicationFactor = 3
		numLogStreams     = 2
		numLogs           = 10
	)

	vt, err := varlogtest.New(
		varlogtest.WithClusterID(clusterID),
		varlogtest.WithReplicationFactor(replicationFactor),
	)
	require.NoError(t, err)

	adm := vt.NewAdminClient()
	vlg := vt.NewLogClient()
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

	first, last, _, err := vlg.PeekLogStream(context.Background(), td.TopicID, lsds[0].LogStreamID)
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 3, GLSN: 5}, first)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 5, GLSN: 9}, last)

	first, last, _, err = vlg.PeekLogStream(context.Background(), td.TopicID, lsds[1].LogStreamID)
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 2, GLSN: 4}, first)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 5, GLSN: 10}, last)

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

func TestVarlogTestAdminMetadataRepository(t *testing.T) {
	const (
		cid       = types.ClusterID(1)
		repfactor = 3
	)

	ctx := context.Background()
	initialMRNs := []varlogpb.MetadataRepositoryNode{
		{
			NodeID:  types.NewNodeIDFromURL("http://127.0.20.1:9091"),
			RaftURL: "http://127.0.20.1:9091",
			RPCAddr: "127.0.20.1:9092",
		},
		{
			NodeID:  types.NewNodeIDFromURL("http://127.0.20.2:9091"),
			RaftURL: "http://127.0.20.2:9091",
			RPCAddr: "127.0.20.2:9092",
		},
		{
			NodeID:  types.NewNodeIDFromURL("http://127.0.20.3:9091"),
			RaftURL: "http://127.0.20.3:9091",
			RPCAddr: "127.0.20.3:9092",
		},
	}

	tcs := []struct {
		name        string
		initialMRNs []varlogpb.MetadataRepositoryNode
		testf       func(t *testing.T, vadm varlog.Admin)
	}{
		{
			name:        "NoInitialMRs",
			initialMRNs: []varlogpb.MetadataRepositoryNode{},
			testf: func(t *testing.T, vadm varlog.Admin) {
				mrns, err := vadm.ListMetadataRepositoryNodes(ctx)
				require.NoError(t, err)
				require.Empty(t, mrns)
			},
		},
		{
			name:        "ListMetadataRepositoryNodes",
			initialMRNs: initialMRNs,
			testf: func(t *testing.T, vadm varlog.Admin) {
				mrns, err := vadm.ListMetadataRepositoryNodes(ctx)
				require.NoError(t, err)
				require.Len(t, mrns, 3)

				for _, initMRN := range initialMRNs {
					require.True(t, slices.ContainsFunc(mrns, func(mrn varlogpb.MetadataRepositoryNode) bool {
						return mrn.NodeID == initMRN.NodeID
					}))
				}

				numLeaders := 0
				for _, mrn := range mrns {
					if mrn.Leader {
						numLeaders++
					}
				}
				require.Equal(t, 1, numLeaders)
			},
		},
		{
			name:        "GetMetadataRepositoryNode",
			initialMRNs: initialMRNs,
			testf: func(t *testing.T, vadm varlog.Admin) {
				for _, initialMRN := range initialMRNs {
					_, err := vadm.GetMetadataRepositoryNode(ctx, initialMRN.NodeID)
					require.NoError(t, err)
				}
			},
		},
		{
			name:        "GetMetadataRepositoryNode",
			initialMRNs: initialMRNs,
			testf: func(t *testing.T, vadm varlog.Admin) {
				rsp, err := vadm.GetMRMembers(ctx)
				require.NoError(t, err)

				require.NotEqual(t, types.InvalidNodeID, rsp.Leader)
				require.EqualValues(t, repfactor, rsp.ReplicationFactor)
				require.Len(t, rsp.Members, len(initialMRNs))
				for _, initialMRN := range initialMRNs {
					require.Contains(t, rsp.Members, initialMRN.NodeID)
				}
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			vt, err := varlogtest.New(
				varlogtest.WithClusterID(cid),
				varlogtest.WithReplicationFactor(repfactor),
				varlogtest.WithInitialMetadataRepositoryNodes(tc.initialMRNs...),
			)
			require.NoError(t, err)

			tc.testf(t, vt.NewAdminClient())
		})
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
