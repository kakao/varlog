package cluster

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/internal/admin"
	"github.com/kakao/varlog/internal/admin/snwatcher"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/x/mlsa"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/tests/it"
)

func TestClientNoLogStream(t *testing.T) {
	// FIXME: detected some leaked goroutines:
	// go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop(0xc0000b0d08)
	// vendor/go.etcd.io/etcd/pkg/logutil/merge_logger.go:173 +0x4bb
	//
	// defer goleak.VerifyNone(t)
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(3),
		it.WithNumberOfStorageNodes(3),
		it.WithNumberOfClients(1),
		it.WithNumberOfTopics(1),
		it.WithVMSOptions(it.NewTestVMSOptions()...),
	)

	defer func() {
		clus.Close(t)
	}()

	topicID := clus.TopicIDs()[0]
	client := clus.ClientAtIndex(t, 0)
	res := client.Append(context.TODO(), topicID, [][]byte{[]byte("foo")})
	require.Error(t, res.Err)
}

func TestClientAppendToSubscribeTo(t *testing.T) {
	const numLogs = 100

	clus := it.NewVarlogCluster(t,
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()...),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
	}()

	topicID := clus.TopicIDs()[0]
	logStreamID := clus.LogStreamIDs(topicID)[0]
	client := clus.ClientAtIndex(t, 0)

	for i := 0; i < numLogs; i++ {
		res := client.AppendTo(context.Background(), topicID, logStreamID, [][]byte{[]byte("foo")})
		require.NoError(t, res.Err)
	}

	// SubscribeTo [2, 1)
	subscriber := client.SubscribeTo(context.Background(), topicID, logStreamID, types.LLSN(2), types.LLSN(1))
	_, err := subscriber.Next()
	require.Error(t, err)
	require.NoError(t, subscriber.Close())

	// SubscribeTo [1, 101)
	subscriber = client.SubscribeTo(context.Background(), topicID, logStreamID, types.MinLLSN, types.LLSN(numLogs+1))
	for i := 0; i < numLogs; i++ {
		logEntry, err := subscriber.Next()
		require.NoError(t, err)
		require.Equal(t, topicID, logEntry.TopicID)
		require.Equal(t, logStreamID, logEntry.LogStreamID)
		require.Equal(t, types.LLSN(i+1), logEntry.LLSN)
	}
	_, err = subscriber.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, subscriber.Close())

	// SubscribeTo [1, max)
	subscriber = client.SubscribeTo(context.Background(), topicID, logStreamID, types.MinLLSN, types.MaxLLSN)
	for i := 0; i < numLogs; i++ {
		logEntry, err := subscriber.Next()
		require.NoError(t, err)
		require.Equal(t, topicID, logEntry.TopicID)
		require.Equal(t, logStreamID, logEntry.LogStreamID)
		require.Equal(t, types.LLSN(i+1), logEntry.LLSN)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := subscriber.Next()
		require.Error(t, err)
		require.NotErrorIs(t, err, io.EOF)
	}()
	time.Sleep(5 * time.Millisecond)
	require.NoError(t, subscriber.Close())
}

func TestClientAppendTo(t *testing.T) {
	// defer goleak.VerifyNone(t)
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(3),
		it.WithNumberOfStorageNodes(3),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()...),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
	}()

	// FIXME: remove this ugly code
	topicID := clus.TopicIDs()[0]
	lsID := clus.LogStreamID(t, topicID, 0)
	client := clus.ClientAtIndex(t, 0)

	res := client.AppendTo(context.TODO(), topicID, lsID+1, [][]byte{[]byte("foo")})
	require.Error(t, res.Err)

	res = client.AppendTo(context.TODO(), topicID, lsID, [][]byte{[]byte("foo")})
	require.NoError(t, res.Err)

	// NOTE: Read API is deprecated.
	// data, err := client.Read(context.Background(), topicID, lsID, res.Metadata[0].GLSN)
	// require.NoError(t, err)
	// require.EqualValues(t, []byte("foo"), data)
}

func TestClientAppend(t *testing.T) {
	// defer goleak.VerifyNone(t)
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(3),
		it.WithNumberOfLogStreams(3),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()...),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
	}()

	topicID := clus.TopicIDs()[0]
	client := clus.ClientAtIndex(t, 0)

	expectedLLSNs := make(map[types.LogStreamID]types.LLSN, clus.NumberOfLogStreams(topicID))
	for _, lsID := range clus.LogStreamIDs(topicID) {
		expectedLLSNs[lsID] = types.MinLLSN
	}
	expectedGLSN := types.MinGLSN

	for _, logStreamID := range clus.LogStreamIDs(topicID) {
		res := client.AppendTo(context.Background(), topicID, logStreamID, [][]byte{nil})
		require.NoError(t, res.Err)
		lem := res.Metadata[0]
		require.Equal(t, expectedGLSN, lem.GLSN)
		expectedGLSN++
		require.Equal(t, expectedLLSNs[lem.LogStreamID], lem.LLSN)
		expectedLLSNs[lem.LogStreamID]++
		require.Equal(t, topicID, lem.TopicID)
		require.Equal(t, logStreamID, lem.LogStreamID)
	}

	for i := 0; i < 10; i++ {
		res := client.Append(context.TODO(), topicID, [][]byte{[]byte("foo")})
		require.NoError(t, res.Err)
		lem := res.Metadata[0]
		require.Equal(t, expectedGLSN, lem.GLSN)
		expectedGLSN++
		require.Equal(t, expectedLLSNs[lem.LogStreamID], lem.LLSN)
		expectedLLSNs[lem.LogStreamID]++
		require.Equal(t, topicID, lem.TopicID)
	}

	// NOTE: Read API is deprecated.
	// require.Condition(t, func() bool {
	// 	for _, lsid := range clus.LogStreamIDs(topicID) {
	// 		if _, errRead := client.Read(context.TODO(), topicID, lsid, types.MinGLSN); errRead == nil {
	//			return true
	//		}
	//	}
	//	return false
	// })

	for _, logStreamID := range clus.LogStreamIDs(topicID) {
		first, last, ok, err := client.PeekLogStream(context.Background(), topicID, logStreamID)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, types.MinLLSN, first.LLSN)
		require.GreaterOrEqual(t, first.GLSN, types.MinGLSN)
		require.GreaterOrEqual(t, last.LLSN, types.MinLLSN)
		require.GreaterOrEqual(t, last.GLSN, types.MinGLSN)
	}
}

func TestClientAppendCancel(t *testing.T) {
	// defer goleak.VerifyNone(t)
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()...),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
	}()

	topicID := clus.TopicIDs()[0]
	client := clus.ClientAtIndex(t, 0)

	var (
		atomicGLSN atomic.Uint64
		wg         sync.WaitGroup
	)
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		expectedGLSN := types.MinGLSN
		for {
			res := client.Append(ctx, topicID, [][]byte{[]byte("foo")})
			if res.Err == nil {
				require.Equal(t, expectedGLSN, res.Metadata[0].GLSN)
				expectedGLSN++
				atomicGLSN.Store(uint64(res.Metadata[0].GLSN))
			} else {
				t.Logf("canceled")
				return
			}
		}
	}()

	for atomicGLSN.Load() < 10 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	wg.Wait()
}

func TestClientSubscribe(t *testing.T) {
	// defer goleak.VerifyNone(t)
	const (
		batchSize = 10
		appendCnt = 10
		nrLogs    = batchSize * appendCnt
	)

	clus := it.NewVarlogCluster(t,
		it.WithNumberOfStorageNodes(3),
		it.WithNumberOfLogStreams(3),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()...),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
	}()

	newMsg := func(glsn types.GLSN) string {
		return fmt.Sprintf("msg-%03d", glsn)
	}

	issuedGLSN := types.InvalidGLSN
	topicID := clus.TopicIDs()[0]
	client := clus.ClientAtIndex(t, 0)
	for i := 0; i < appendCnt; i++ {
		batch := make([][]byte, batchSize)
		for j := 0; j < batchSize; j++ {
			issuedGLSN++
			msg := newMsg(issuedGLSN)
			batch[j] = []byte(msg)
		}
		res := client.Append(context.TODO(), topicID, batch)
		require.NoError(t, res.Err)
		require.Len(t, res.Metadata, batchSize)
		require.Equal(t, issuedGLSN, res.Metadata[len(res.Metadata)-1].GLSN)
		require.Equal(t, issuedGLSN-types.GLSN(batchSize)+1, res.Metadata[0].GLSN)
	}

	errc := make(chan error, nrLogs)
	expectedGLSN := types.MinGLSN
	subscribeCloser, err := client.Subscribe(context.TODO(), topicID, types.GLSN(1), types.GLSN(nrLogs+1), func(le varlogpb.LogEntry, err error) {
		if err != nil {
			require.ErrorIs(t, io.EOF, err)
			close(errc)
			return
		}
		assert.Equal(t, expectedGLSN, le.GLSN)
		expectedMsg := newMsg(expectedGLSN)
		require.Equal(t, expectedMsg, string(le.Data))
		expectedGLSN++
		errc <- err
	})
	require.NoError(t, err)
	defer subscribeCloser()

	for e := range errc {
		if e != nil {
			require.ErrorIs(t, io.EOF, e)
		}
	}
}

func TestClientTrim(t *testing.T) {
	// defer goleak.VerifyNone(t)
	const (
		nrLogs  = 10
		trimPos = types.GLSN(5)
	)

	clus := it.NewVarlogCluster(t,
		it.WithNumberOfStorageNodes(3),
		it.WithNumberOfLogStreams(3),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()...),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
	}()

	topicID := clus.TopicIDs()[0]
	client := clus.ClientAtIndex(t, 0)
	expectedGLSN := types.GLSN(1)
	for i := 0; i < nrLogs; i++ {
		res := client.Append(context.TODO(), topicID, [][]byte{[]byte("foo")})
		require.NoError(t, res.Err)
		require.Equal(t, expectedGLSN, res.Metadata[0].GLSN)
		expectedGLSN++
	}

	err := client.Trim(context.Background(), topicID, trimPos, varlog.TrimOption{})
	require.NoError(t, err)

	// actual deletion in SN is asynchronous.
	require.Eventually(t, func() bool {
		errC := make(chan error)
		nopOnNext := func(le varlogpb.LogEntry, err error) {
			t.Logf("subscribe: le=%+v err=%v", le, err)
			isErr := err != nil
			errC <- err
			if isErr {
				close(errC)
			}
		}
		closer, err := client.Subscribe(context.TODO(), topicID, types.MinGLSN, trimPos, nopOnNext)
		require.NoError(t, err)
		defer closer()

		isErr := false
		for err := range errC {
			isErr = isErr || (err != nil && err != io.EOF)
		}
		return isErr
	}, time.Second, 10*time.Millisecond)

	// subscribe remains
	ch := make(chan varlogpb.LogEntry)
	onNext := func(logEntry varlogpb.LogEntry, err error) {
		if err != nil {
			close(ch)
			return
		}
		ch <- logEntry
	}
	closer, err := client.Subscribe(context.TODO(), topicID, trimPos+1, types.GLSN(nrLogs), onNext)
	require.NoError(t, err)
	defer closer()
	expectedGLSN = trimPos + 1
	for logEntry := range ch {
		require.Equal(t, expectedGLSN, logEntry.GLSN)
		expectedGLSN++
	}
}

func TestVarlogSubscribeWithSNFail(t *testing.T) {
	// defer goleak.VerifyNone(t)

	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(3),
		it.WithNumberOfLogStreams(3),
		it.WithNumberOfClients(5),
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		topicID := env.TopicIDs()[0]
		client := env.ClientAtIndex(t, 0)

		nrLogs := 64
		for i := 0; i < nrLogs; i++ {
			res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
			So(res.Err, ShouldBeNil)
		}

		Convey("When SN fail", func(ctx C) {
			snID := env.StorageNodeIDAtIndex(t, 0)

			env.CloseSN(t, snID)
			env.CloseSNClientOf(t, snID)

			Convey("Then it should be able to subscribe", func(ctx C) {
				errc := make(chan error, nrLogs)
				expectedGLSN := types.GLSN(1)
				subscribeCloser, err := client.Subscribe(context.TODO(), topicID, types.GLSN(1), types.GLSN(nrLogs+1), func(le varlogpb.LogEntry, err error) {
					if err != nil {
						require.ErrorIs(t, io.EOF, err)
						defer close(errc)
						return
					}
					assert.Equal(t, expectedGLSN, le.GLSN)
					expectedGLSN++
					errc <- err
				})
				require.NoError(t, err)
				defer subscribeCloser()

				for e := range errc {
					if e != nil {
						require.ErrorIs(t, io.EOF, e)
					}
				}
			})
		})
	}))
}

func TestVarlogSubscribeWithAddLS(t *testing.T) {
	// defer goleak.VerifyNone(t)
	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(5),
		it.WithNumberOfLogStreams(3),
		it.WithNumberOfClients(2),
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		nrLogs := 10

		Convey("When add LogStream during subscribe", func(ctx C) {
			topicID := env.TopicIDs()[0]
			client := env.ClientAtIndex(t, 0)
			errc := make(chan error, nrLogs)
			expectedGLSN := types.GLSN(1)
			subscribeCloser, err := client.Subscribe(context.TODO(), topicID, types.GLSN(1), types.GLSN(nrLogs+1), func(le varlogpb.LogEntry, err error) {
				if err != nil {
					require.ErrorIs(t, io.EOF, err)
					defer close(errc)
					return
				}
				assert.Equal(t, expectedGLSN, le.GLSN)
				expectedGLSN++
				errc <- err
			})
			require.NoError(t, err)
			defer subscribeCloser()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				client := env.ClientAtIndex(t, 1)

				for i := 0; i < nrLogs/2; i++ {
					res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
					require.NoError(t, res.Err)
				}

				topicID := env.TopicIDs()[0]
				env.AddLS(t, topicID)

				for i := 0; i < nrLogs/2; i++ {
					res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
					require.NoError(t, res.Err)
				}
			}()
			wg.Wait()

			Convey("Then it should be able to subscribe", func(ctx C) {
				for e := range errc {
					if e != nil {
						require.ErrorIs(t, io.EOF, e)
					}
				}
			})
		})
	}))
}

func TestVarlogSubscribeWithUpdateLS(t *testing.T) {
	// defer goleak.VerifyNone(t)
	const (
		tick            = snwatcher.DefaultTick
		reportInterval  = tick
		hbCheckDeadline = tick
		hbTimeout       = 5 * tick
	)
	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(5),
		it.WithNumberOfLogStreams(3),
		it.WithNumberOfClients(5),
		it.WithNumberOfTopics(1),
		it.WithVMSOptions(
			admin.WithStorageNodeWatcherOptions(
				snwatcher.WithTick(tick),
				snwatcher.WithHeartbeatTimeout(hbTimeout),
				snwatcher.WithHeartbeatCheckDeadline(hbCheckDeadline),
				snwatcher.WithReportInterval(reportInterval),
			),
		),
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		nrLogs := 128

		Convey("When update LogStream during subscribe", func(ctx C) {
			topicID := env.TopicIDs()[0]
			client := env.ClientAtIndex(t, 0)
			errc := make(chan error, nrLogs)
			expectedGLSN := types.GLSN(1)
			subscribeCloser, err := client.Subscribe(context.TODO(), topicID, types.GLSN(1), types.GLSN(nrLogs+1), func(le varlogpb.LogEntry, err error) {
				if err != nil {
					require.ErrorIs(t, io.EOF, err)
					close(errc)
					return
				}
				assert.Equal(t, expectedGLSN, le.GLSN)
				expectedGLSN++
				errc <- err
			})
			require.NoError(t, err)
			defer subscribeCloser()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()

				client := env.ClientAtIndex(t, 1)
				for i := 0; i < nrLogs/2; i++ {
					res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
					require.NoError(t, res.Err)
				}

				addedSN := env.AddSN(t)

				topicID := env.TopicIDs()[0]
				lsID := env.LogStreamID(t, topicID, 0)
				snID := env.PrimaryStorageNodeIDOf(t, lsID)

				env.CloseSN(t, snID)
				env.CloseSNClientOf(t, snID)

				require.Eventually(t, func() bool {
					meta := env.GetMetadata(t)
					lsdesc := meta.GetLogStream(lsID)
					return lsdesc.Status == varlogpb.LogStreamStatusSealed
				}, hbTimeout*2, tick)

				env.UpdateLS(t, topicID, lsID, snID, addedSN)

				for i := 0; i < nrLogs/4; i++ {
					res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
					require.NoError(t, res.Err)
				}

				require.Eventually(t, func() bool {
					lsDesc, err := env.Unseal(topicID, lsID)
					if err != nil {
						return false
					}

					return lsDesc.Status == varlogpb.LogStreamStatusRunning
				}, 10*tick, tick)

				for i := 0; i < nrLogs/4; i++ {
					res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
					require.NoError(t, res.Err)
				}
			}()
			wg.Wait()

			Convey("Then it should be able to subscribe", func(ctx C) {
				for e := range errc {
					if e != nil {
						require.ErrorIs(t, io.EOF, e)
					}
				}
			})
		})
	}))
}

func TestClientPeekLogStream(t *testing.T) {
	clus := it.NewVarlogCluster(t,
		it.WithNumberOfStorageNodes(2),
		it.WithReplicationFactor(2),
		it.WithNumberOfTopics(1),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()...),
	)
	defer clus.Close(t)

	tpid := clus.TopicIDs()[0]
	lsid := clus.LogStreamIDs(tpid)[0]
	client := clus.ClientAtIndex(t, 0)

	first, last, ok, err := client.PeekLogStream(context.Background(), tpid, lsid)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, first.Invalid())
	require.True(t, last.Invalid())

	res := client.Append(context.Background(), tpid, [][]byte{nil})
	require.NoError(t, res.Err)

	first, last, ok, err = client.PeekLogStream(context.Background(), tpid, lsid)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, varlogpb.LogSequenceNumber{
		LLSN: 1, GLSN: 1,
	}, first)
	require.Equal(t, varlogpb.LogSequenceNumber{
		LLSN: 1, GLSN: 1,
	}, last)

	idx := int(time.Now().UnixNano() % 2)
	clus.CloseSN(t, clus.StorageNodeIDAtIndex(t, idx))

	first, last, ok, err = client.PeekLogStream(context.Background(), tpid, lsid)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, varlogpb.LogSequenceNumber{
		LLSN: 1, GLSN: 1,
	}, first)
	require.Equal(t, varlogpb.LogSequenceNumber{
		LLSN: 1, GLSN: 1,
	}, last)

	idx = (idx + 1) % 2
	clus.CloseSN(t, clus.StorageNodeIDAtIndex(t, idx))

	_, _, ok, err = client.PeekLogStream(context.Background(), tpid, lsid)
	require.Error(t, err)
	require.False(t, ok)
}

func TestClientAppendWithAllowedLogStream(t *testing.T) {
	const numLogs = 100
	const numLogStreams = 10

	clus := it.NewVarlogCluster(t,
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(numLogStreams),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()...),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
	}()

	topicID := clus.TopicIDs()[0]
	logStreamID := clus.LogStreamIDs(topicID)[0]
	sealedLogStreamID := clus.LogStreamIDs(topicID)[1]
	client := clus.ClientAtIndex(t, 0)

	allowedLogStreams := make(map[types.LogStreamID]struct{})
	allowedLogStreams[logStreamID] = struct{}{}
	allowedLogStreams[sealedLogStreamID] = struct{}{}

	rsp, err := clus.Seal(topicID, sealedLogStreamID)
	require.NoError(t, err)
	require.Len(t, rsp.LogStreams, 1)
	require.Equal(t, varlogpb.LogStreamStatusSealed, rsp.LogStreams[0].Status)

	for i := 0; i < numLogs; i++ {
		res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")}, varlog.WithAllowedLogStreams(allowedLogStreams))
		require.NoError(t, res.Err)
	}

	// SubscribeTo [1, 101)
	subscriber := client.SubscribeTo(context.Background(), topicID, logStreamID, types.MinLLSN, types.LLSN(numLogs+1))
	for i := 0; i < numLogs; i++ {
		logEntry, err := subscriber.Next()
		require.NoError(t, err)
		require.Equal(t, topicID, logEntry.TopicID)
		require.Equal(t, logStreamID, logEntry.LogStreamID)
		require.Equal(t, types.LLSN(i+1), logEntry.LLSN)
	}
	_, err = subscriber.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, subscriber.Close())
}

func TestLogStreamAppender(t *testing.T) {
	const (
		pipelineSize    = 2
		calls           = pipelineSize * 5
		batchSize       = 2
		tick            = snwatcher.DefaultTick
		reportInterval  = tick
		hbCheckDeadline = tick
		hbTimeout       = 5 * tick
	)

	tcs := []struct {
		name  string
		testf func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log)
	}{
		{
			name: "CloseBeforeAppendLogs",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				lsa, err := vcli.NewLogStreamAppender(tpid, lsid, varlog.WithPipelineSize(pipelineSize))
				require.NoError(t, err)
				lsa.Close()
				err = lsa.AppendBatch([][]byte{[]byte("foo")}, nil)
				require.Equal(t, varlog.ErrClosed, err)
			},
		},
		{
			name: "CloseAfterProcessingCallbacks",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				var (
					llsn types.LLSN
					glsn types.GLSN
					wg   sync.WaitGroup
				)
				cb := func(metas []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
					assert.Len(t, metas, batchSize)
					assert.Less(t, llsn, metas[batchSize-1].LLSN)
					assert.Less(t, glsn, metas[batchSize-1].GLSN)
					llsn = metas[batchSize-1].LLSN
					glsn = metas[batchSize-1].GLSN
				}

				lsa, err := vcli.NewLogStreamAppender(tpid, lsid, varlog.WithPipelineSize(pipelineSize))
				require.NoError(t, err)
				defer func() {
					wg.Wait()
					lsa.Close()
				}()

				wg.Add(calls)
				for i := 0; i < calls; i++ {
					data := make([][]byte, batchSize)
					for j := 0; j < batchSize; j++ {
						data[j] = []byte(fmt.Sprintf("%d,%d", i, j))
					}
					err := lsa.AppendBatch(data, cb)
					require.NoError(t, err)
				}
			},
		},
		{
			name: "CloseWhileProcessingCallbacks",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				var (
					llsn types.LLSN
					glsn types.GLSN
					wg   sync.WaitGroup
				)
				cb := func(metas []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					if err != nil {
						assert.Equal(t, varlog.ErrClosed, err)
						return
					}
					assert.NoError(t, err)
					assert.Len(t, metas, batchSize)
					assert.Less(t, llsn, metas[batchSize-1].LLSN)
					assert.Less(t, glsn, metas[batchSize-1].GLSN)
					llsn = metas[batchSize-1].LLSN
					glsn = metas[batchSize-1].GLSN
				}

				lsa, err := vcli.NewLogStreamAppender(tpid, lsid, varlog.WithPipelineSize(pipelineSize))
				require.NoError(t, err)
				defer func() {
					lsa.Close()
					wg.Wait()
				}()

				wg.Add(calls)
				for i := 0; i < calls; i++ {
					data := make([][]byte, batchSize)
					for j := 0; j < batchSize; j++ {
						data[j] = []byte(fmt.Sprintf("%d,%d", i, j))
					}
					err := lsa.AppendBatch(data, cb)
					require.NoError(t, err)
				}
			},
		},
		{
			name: "CloseInCallback",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				lsa, err := vcli.NewLogStreamAppender(tpid, lsid, varlog.WithPipelineSize(pipelineSize))
				require.NoError(t, err)
				defer func() {
					lsa.Close()
				}()

				var wg sync.WaitGroup
				dataBatch := [][]byte{[]byte("foo")}
				wg.Add(1)
				err = lsa.AppendBatch(dataBatch, func(_ []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
					lsa.Close()
				})
				require.NoError(t, err)
				wg.Wait()

				err = lsa.AppendBatch(dataBatch, func([]varlogpb.LogEntryMeta, error) {
					assert.Fail(t, "unexpected callback")
				})
				require.Error(t, err)
			},
		},
		{
			name: "AppendChain_DoNotDoThis",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				var called atomic.Int32

				lsa, err := vcli.NewLogStreamAppender(tpid, lsid, varlog.WithPipelineSize(pipelineSize))
				require.NoError(t, err)
				defer func() {
					lsa.Close()
					require.EqualValues(t, calls, called.Load())
				}()

				dataBatch := [][]byte{[]byte("foo")}
				callback := testCallbackGen(t, lsa, dataBatch, calls, &called)
				err = lsa.AppendBatch(dataBatch, callback)
				require.NoError(t, err)

				require.Eventually(t, func() bool {
					return called.Load() == calls
				}, 10*tick, tick)
			},
		},
		{
			name: "ConcurrentAppendBatch",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				lsa, err := vcli.NewLogStreamAppender(tpid, lsid, varlog.WithPipelineSize(pipelineSize))
				require.NoError(t, err)
				defer func() {
					lsa.Close()
				}()

				var wg sync.WaitGroup
				wg.Add(calls * 2)
				expected := 0
				dataBatch := [][]byte{[]byte("foo")}
				cb := func(metas []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
					assert.Len(t, metas, 1)
					expected++
					assert.EqualValues(t, expected, metas[0].LLSN)
					assert.EqualValues(t, expected, metas[0].GLSN)
				}
				for i := 0; i < calls; i++ {
					go func() {
						defer wg.Done()
						err := lsa.AppendBatch(dataBatch, cb)
						require.NoError(t, err)
					}()
				}
				wg.Wait()
			},
		},
		{
			name: "CallTimeoutCausedBySemaphore",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				const (
					callTimeout         = 500 * time.Millisecond
					blockedPipelineSize = 1
				)

				lsa, err := vcli.NewLogStreamAppender(tpid, lsid,
					varlog.WithPipelineSize(blockedPipelineSize),
					varlog.WithCallTimeout(callTimeout),
				)
				require.NoError(t, err)
				defer lsa.Close()

				var wg sync.WaitGroup
				dataBatch := [][]byte{[]byte("foo")}

				wg.Add(1)
				err = lsa.AppendBatch(dataBatch, func(_ []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
					time.Sleep(callTimeout * 2)
				})
				require.NoError(t, err)

				err = lsa.AppendBatch(dataBatch, func([]varlogpb.LogEntryMeta, error) {
					assert.Fail(t, "unexpected callback")
				})
				require.Error(t, err)
				require.Equal(t, varlog.ErrCallTimeout, err)

				wg.Wait()
			},
		},
		{
			name: "CallTimeoutCausedSlowCallback",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				const callTimeout = 500 * time.Millisecond

				lsa, err := vcli.NewLogStreamAppender(tpid, lsid,
					varlog.WithCallTimeout(callTimeout),
					varlog.WithPipelineSize(5),
				)
				require.NoError(t, err)
				defer lsa.Close()

				var wg sync.WaitGroup
				dataBatch := [][]byte{[]byte("foo")}

				var failfast atomic.Bool
				for err == nil {
					wg.Add(1)
					err = lsa.AppendBatch(dataBatch, func(_ []varlogpb.LogEntryMeta, cerr error) {
						defer wg.Done()
						if cerr != nil {
							assert.Equal(t, varlog.ErrCallTimeout, cerr)
							failfast.Store(true)
							return
						}
						time.Sleep(callTimeout * 2)
					})
					if err == nil {
						require.True(t, failfast.CompareAndSwap(false, false))
					} else {
						wg.Done()
						require.Equal(t, varlog.ErrCallTimeout, err)
					}
					time.Sleep(callTimeout)
				}

				wg.Wait()
			},
		},
		{
			name: "Manager_NoSuchTopic",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				mgr := mlsa.New(vcli)
				_, err := mgr.Get(tpid+1, lsid)
				require.Error(t, err)

				_, err = mgr.Any(tpid+1, nil)
				require.Error(t, err)
			},
		},
		{
			name: "Manager_NoSuchLogStream",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				mgr := mlsa.New(vcli)
				_, err := mgr.Get(tpid, lsid+1)
				require.Error(t, err)

				_, err = mgr.Any(tpid, map[types.LogStreamID]struct{}{
					lsid + 1: {},
				})
				require.Error(t, err)
			},
		},
		{
			name: "Manager_AppendBatch",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				mgr := mlsa.New(vcli)

				lsa1, err := mgr.Get(tpid, lsid)
				require.NoError(t, err)
				lsa2, err := mgr.Get(tpid, lsid)
				require.NoError(t, err)

				var wg sync.WaitGroup
				dataBatch := [][]byte{[]byte("foo")}

				wg.Add(2)
				err = lsa1.AppendBatch(dataBatch, func(_ []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
				})
				require.NoError(t, err)
				err = lsa2.AppendBatch(dataBatch, func(_ []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
				})
				require.NoError(t, err)
				wg.Wait()

				lsa1.Close()
				lsa2.Close()
			},
		},
		{
			name: "Manager_Close",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				mgr := mlsa.New(vcli)

				lsa1, err := mgr.Get(tpid, lsid)
				require.NoError(t, err)
				lsa2, err := mgr.Get(tpid, lsid)
				require.NoError(t, err)

				dataBatch := [][]byte{[]byte("foo")}

				lsa1.Close()
				err = lsa1.AppendBatch(dataBatch, func([]varlogpb.LogEntryMeta, error) {
					assert.Fail(t, "unexpected callback")
				})
				require.Error(t, err)
				err = lsa2.AppendBatch(dataBatch, func([]varlogpb.LogEntryMeta, error) {
					assert.Fail(t, "unexpected callback")
				})
				require.Error(t, err)
			},
		},
		{
			name: "Manager_CloseAndGet",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				mgr := mlsa.New(vcli)

				lsa1, err := mgr.Get(tpid, lsid)
				require.NoError(t, err)
				lsa2, err := mgr.Get(tpid, lsid)
				require.NoError(t, err)

				lsa1.Close()
				lsa1, err = mgr.Get(tpid, lsid)
				require.NoError(t, err)

				var wg sync.WaitGroup
				dataBatch := [][]byte{[]byte("foo")}
				wg.Add(1)
				err = lsa1.AppendBatch(dataBatch, func(_ []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
				})
				require.NoError(t, err)
				err = lsa2.AppendBatch(dataBatch, func([]varlogpb.LogEntryMeta, error) {
					assert.Fail(t, "unexpected callback")
				})
				require.Error(t, err)
				wg.Wait()

				lsa1.Close()
			},
		},
		{
			name: "Manager_Any",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				mgr := mlsa.New(vcli)

				lsa, err := mgr.Any(tpid, map[types.LogStreamID]struct{}{
					lsid: {},
				})
				require.NoError(t, err)

				var wg sync.WaitGroup
				dataBatch := [][]byte{[]byte("foo")}
				wg.Add(1)
				err = lsa.AppendBatch(dataBatch, func(_ []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
				})
				require.NoError(t, err)
				wg.Wait()

				lsa.Close()
			},
		},
		{
			name: "Manager_Clear",
			testf: func(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, vcli varlog.Log) {
				mgr := mlsa.New(vcli)

				lsa, err := mgr.Get(tpid, lsid)
				require.NoError(t, err)

				var wg sync.WaitGroup
				dataBatch := [][]byte{[]byte("foo")}
				wg.Add(1)
				err = lsa.AppendBatch(dataBatch, func(_ []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
				})
				require.NoError(t, err)
				wg.Wait()

				mgr.Clear()
				err = lsa.AppendBatch(dataBatch, func([]varlogpb.LogEntryMeta, error) {
					assert.Fail(t, "unexpected callback")
				})
				require.Error(t, err)

				lsa, err = mgr.Get(tpid, lsid)
				require.NoError(t, err)

				wg.Add(1)
				err = lsa.AppendBatch(dataBatch, func(_ []varlogpb.LogEntryMeta, err error) {
					defer wg.Done()
					assert.NoError(t, err)
				})
				require.NoError(t, err)
				wg.Wait()

				mgr.Clear()
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			clus := it.NewVarlogCluster(t,
				it.WithReplicationFactor(3),
				it.WithNumberOfStorageNodes(3),
				it.WithNumberOfLogStreams(1),
				it.WithNumberOfClients(1),
				it.WithVMSOptions(it.NewTestVMSOptions()...),
				it.WithNumberOfTopics(1),
				it.WithVMSOptions(
					admin.WithStorageNodeWatcherOptions(
						snwatcher.WithTick(tick),
						snwatcher.WithHeartbeatTimeout(hbTimeout),
						snwatcher.WithHeartbeatCheckDeadline(hbCheckDeadline),
						snwatcher.WithReportInterval(reportInterval),
					),
				),
			)
			defer clus.Close(t)

			tpid := clus.TopicIDs()[0]
			lsid := clus.LogStreamID(t, tpid, 0)
			client := clus.ClientAtIndex(t, 0)

			tc.testf(t, tpid, lsid, client)
		})
	}
}

func testCallbackGen(t *testing.T, lsa varlog.LogStreamAppender, dataBatch [][]byte, limit int32, called *atomic.Int32) varlog.BatchCallback {
	return func(metas []varlogpb.LogEntryMeta, err error) {
		require.NoError(t, err)
		called.Add(1)
		if called.Load() < limit {
			err = lsa.AppendBatch(dataBatch, testCallbackGen(t, lsa, dataBatch, limit, called))
			require.NoError(t, err)
		}
	}
}
