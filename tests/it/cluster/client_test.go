package cluster

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/tests/it"
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
		it.WithVMSOptions(it.NewTestVMSOptions()),
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	topicID := clus.TopicIDs()[0]
	client := clus.ClientAtIndex(t, 0)
	_, err := client.Append(context.TODO(), topicID, []byte("foo"))
	require.Error(t, err)
}

func TestClientAppendTo(t *testing.T) {
	// defer goleak.VerifyNone(t)
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(3),
		it.WithNumberOfStorageNodes(3),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	// FIXME: remove this ugly code
	topicID := clus.TopicIDs()[0]
	lsID := clus.LogStreamID(t, topicID, 0)
	client := clus.ClientAtIndex(t, 0)

	_, err := client.AppendTo(context.TODO(), topicID, lsID+1, []byte("foo"))
	require.Error(t, err)

	lem, err := client.AppendTo(context.TODO(), topicID, lsID, []byte("foo"))
	require.NoError(t, err)

	data, err := client.Read(context.Background(), topicID, lsID, lem.GLSN)
	require.NoError(t, err)
	require.EqualValues(t, []byte("foo"), data)
}

func TestClientAppend(t *testing.T) {
	// defer goleak.VerifyNone(t)
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(3),
		it.WithNumberOfLogStreams(3),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	topicID := clus.TopicIDs()[0]
	client := clus.ClientAtIndex(t, 0)

	expectedLLSNs := make(map[types.LogStreamID]types.LLSN, clus.NumberOfLogStreams(topicID))
	for _, lsID := range clus.LogStreamIDs(topicID) {
		expectedLLSNs[lsID] = types.MinLLSN
	}
	expectedGLSN := types.MinGLSN
	for i := 0; i < 10; i++ {
		lem, err := client.Append(context.TODO(), topicID, []byte("foo"))
		require.NoError(t, err)
		require.Equal(t, expectedGLSN, lem.GLSN)
		expectedGLSN++
		require.Equal(t, expectedLLSNs[lem.LogStreamID], lem.LLSN)
		expectedLLSNs[lem.LogStreamID]++
		require.Equal(t, topicID, lem.TopicID)
	}

	require.Condition(t, func() bool {
		for _, lsid := range clus.LogStreamIDs(topicID) {
			if _, errRead := client.Read(context.TODO(), topicID, lsid, 1); errRead == nil {
				return true
			}
		}
		return false
	})
}

func TestClientAppendCancel(t *testing.T) {
	// defer goleak.VerifyNone(t)
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	topicID := clus.TopicIDs()[0]
	client := clus.ClientAtIndex(t, 0)

	var (
		atomicGLSN types.AtomicGLSN
		wg         sync.WaitGroup
	)
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		expectedGLSN := types.MinGLSN
		for {
			lem, err := client.Append(ctx, topicID, []byte("foo"))
			if err == nil {
				require.Equal(t, expectedGLSN, lem.GLSN)
				expectedGLSN++
				atomicGLSN.Store(lem.GLSN)
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
	//defer goleak.VerifyNone(t)
	const nrLogs = 10

	clus := it.NewVarlogCluster(t,
		it.WithNumberOfStorageNodes(3),
		it.WithNumberOfLogStreams(3),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	topicID := clus.TopicIDs()[0]
	client := clus.ClientAtIndex(t, 0)
	for i := 0; i < nrLogs; i++ {
		_, err := client.Append(context.TODO(), topicID, []byte("foo"))
		require.NoError(t, err)
	}

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
		it.WithVMSOptions(it.NewTestVMSOptions()),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	topicID := clus.TopicIDs()[0]
	client := clus.ClientAtIndex(t, 0)
	expectedGLSN := types.GLSN(1)
	for i := 0; i < nrLogs; i++ {
		lem, err := client.Append(context.TODO(), topicID, []byte("foo"))
		require.NoError(t, err)
		require.Equal(t, expectedGLSN, lem.GLSN)
		expectedGLSN++
	}

	err := client.Trim(context.Background(), topicID, trimPos, varlog.TrimOption{})
	require.NoError(t, err)

	// actual deletion in SN is asynchronous.
	require.Eventually(t, func() bool {
		errC := make(chan error)
		nopOnNext := func(le varlogpb.LogEntry, err error) {
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
	//defer goleak.VerifyNone(t)

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
			_, err := client.Append(context.Background(), topicID, []byte("foo"))
			So(err, ShouldBeNil)
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
	//defer goleak.VerifyNone(t)
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
					_, err := client.Append(context.Background(), topicID, []byte("foo"))
					require.NoError(t, err)
				}

				topicID := env.TopicIDs()[0]
				env.AddLS(t, topicID)

				for i := 0; i < nrLogs/2; i++ {
					_, err := client.Append(context.Background(), topicID, []byte("foo"))
					require.NoError(t, err)
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
	//defer goleak.VerifyNone(t)
	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(5),
		it.WithNumberOfLogStreams(3),
		it.WithNumberOfClients(5),
		it.WithNumberOfTopics(1),
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
					defer close(errc)
					return
				}
				assert.Equal(t, expectedGLSN, le.GLSN)
				expectedGLSN++
				errc <- err
			})
			require.NoError(t, err)
			defer subscribeCloser()

			go func() {
				client := env.ClientAtIndex(t, 1)

				for i := 0; i < nrLogs/2; i++ {
					_, err := client.Append(context.Background(), topicID, []byte("foo"))
					require.NoError(t, err)
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
				}, 5*time.Second, 10*time.Millisecond)

				env.UpdateLS(t, topicID, lsID, snID, addedSN)

				for i := 0; i < nrLogs/2; i++ {
					_, err := client.Append(context.Background(), topicID, []byte("foo"))
					require.NoError(t, err)
				}
			}()

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
