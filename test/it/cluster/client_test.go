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

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/test/it"
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
		it.WithVMSOptions(it.NewTestVMSOptions()),
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	client := clus.ClientAtIndex(t, 0)
	_, err := client.Append(context.TODO(), []byte("foo"))
	require.Error(t, err)
}

func TestClientAppendTo(t *testing.T) {
	// defer goleak.VerifyNone(t)
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(3),
		it.WithNumberOfStorageNodes(3),
		it.WithNumberOfLogStreams(3),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()),
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	// FIXME: remove this ugly code
	lsIDs := clus.LogStreamIDs()
	lsID := lsIDs[len(lsIDs)-1]
	client := clus.ClientAtIndex(t, 0)

	_, err := client.AppendTo(context.TODO(), lsID+1, []byte("foo"))
	require.Error(t, err)

	glsn, err := client.AppendTo(context.TODO(), lsID, []byte("foo"))
	require.NoError(t, err)

	data, err := client.Read(context.Background(), lsID, glsn)
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
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	client := clus.ClientAtIndex(t, 0)

	expectedGLSN := types.MinGLSN
	for i := 0; i < 10; i++ {
		glsn, err := client.Append(context.TODO(), []byte("foo"))
		require.NoError(t, err)
		require.Equal(t, expectedGLSN, glsn)
		expectedGLSN++
	}

	require.Condition(t, func() bool {
		for _, lsid := range clus.LogStreamIDs() {
			if _, errRead := client.Read(context.TODO(), lsid, 1); errRead == nil {
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
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

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
			glsn, err := client.Append(ctx, []byte("foo"))
			if err == nil {
				require.Equal(t, expectedGLSN, glsn)
				expectedGLSN++
				atomicGLSN.Store(glsn)
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
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	client := clus.ClientAtIndex(t, 0)
	for i := 0; i < nrLogs; i++ {
		_, err := client.Append(context.TODO(), []byte("foo"))
		require.NoError(t, err)
	}

	errc := make(chan error, nrLogs)
	expectedGLSN := types.GLSN(1)
	subscribeCloser, err := client.Subscribe(context.TODO(), types.GLSN(1), types.GLSN(nrLogs+1), func(le types.LogEntry, err error) {
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
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	client := clus.ClientAtIndex(t, 0)
	expectedGLSN := types.GLSN(1)
	for i := 0; i < nrLogs; i++ {
		glsn, err := client.Append(context.TODO(), []byte("foo"))
		require.NoError(t, err)
		require.Equal(t, expectedGLSN, glsn)
		expectedGLSN++
	}

	err := client.Trim(context.Background(), trimPos, varlog.TrimOption{})
	require.NoError(t, err)

	// actual deletion in SN is asynchronous.
	require.Eventually(t, func() bool {
		errC := make(chan error)
		nopOnNext := func(le types.LogEntry, err error) {
			isErr := err != nil
			errC <- err
			if isErr {
				close(errC)
			}
		}
		closer, err := client.Subscribe(context.TODO(), types.MinGLSN, trimPos, nopOnNext)
		require.NoError(t, err)
		defer closer()

		isErr := false
		for err := range errC {
			isErr = isErr || (err != nil && err != io.EOF)
		}
		return isErr
	}, time.Second, 10*time.Millisecond)

	// subscribe remains
	ch := make(chan types.LogEntry)
	onNext := func(logEntry types.LogEntry, err error) {
		if err != nil {
			close(ch)
			return
		}
		ch <- logEntry
	}
	closer, err := client.Subscribe(context.TODO(), trimPos+1, types.GLSN(nrLogs), onNext)
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
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		client := env.ClientAtIndex(t, 0)

		nrLogs := 64
		for i := 0; i < nrLogs; i++ {
			_, err := client.Append(context.Background(), []byte("foo"))
			So(err, ShouldBeNil)
		}

		Convey("When SN fail", func(ctx C) {
			snID := env.StorageNodeIDAtIndex(t, 0)

			env.CloseSN(t, snID)
			env.CloseSNClientOf(t, snID)

			Convey("Then it should be able to subscribe", func(ctx C) {
				errc := make(chan error, nrLogs)
				expectedGLSN := types.GLSN(1)
				subscribeCloser, err := client.Subscribe(context.TODO(), types.GLSN(1), types.GLSN(nrLogs+1), func(le types.LogEntry, err error) {
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
		it.WithNumberOfClients(5),
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		nrLogs := 128

		Convey("When add LogStream during subscribe", func(ctx C) {
			client := env.ClientAtIndex(t, 0)
			errc := make(chan error, nrLogs)
			expectedGLSN := types.GLSN(1)
			subscribeCloser, err := client.Subscribe(context.TODO(), types.GLSN(1), types.GLSN(nrLogs+1), func(le types.LogEntry, err error) {
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
					_, err := client.Append(context.Background(), []byte("foo"))
					require.NoError(t, err)
				}

				snID := env.StorageNodeIDAtIndex(t, 0)
				env.CloseSN(t, snID)
				env.CloseSNClientOf(t, snID)

				env.AddLS(t)

				for i := 0; i < nrLogs/2; i++ {
					_, err := client.Append(context.Background(), []byte("foo"))
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

func TestVarlogSubscribeWithUpdateLS(t *testing.T) {
	//defer goleak.VerifyNone(t)

	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(5),
		it.WithNumberOfLogStreams(3),
		it.WithNumberOfClients(5),
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		nrLogs := 128

		Convey("When update LogStream during subscribe", func(ctx C) {
			client := env.ClientAtIndex(t, 0)
			errc := make(chan error, nrLogs)
			expectedGLSN := types.GLSN(1)
			subscribeCloser, err := client.Subscribe(context.TODO(), types.GLSN(1), types.GLSN(nrLogs+1), func(le types.LogEntry, err error) {
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
					_, err := client.Append(context.Background(), []byte("foo"))
					require.NoError(t, err)
				}

				addedSN := env.AddSN(t)

				lsID := env.LogStreamID(t, 0)
				snID := env.PrimaryStorageNodeIDOf(t, lsID)

				env.CloseSN(t, snID)
				env.CloseSNClientOf(t, snID)

				require.Eventually(t, func() bool {
					meta := env.GetMetadata(t)
					lsdesc := meta.GetLogStream(lsID)
					return lsdesc.Status == varlogpb.LogStreamStatusSealed
				}, 5*time.Second, 10*time.Millisecond)

				env.UpdateLS(t, lsID, snID, addedSN)

				for i := 0; i < nrLogs/2; i++ {
					_, err := client.Append(context.Background(), []byte("foo"))
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
