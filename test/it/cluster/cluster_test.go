package cluster

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/test/it"
)

func TestAppendLogs(t *testing.T) {
	const numAppend = 100

	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(10),
		it.WithNumberOfClients(10),
	)
	defer clus.Close(t)

	var (
		muMaxGLSN sync.Mutex
		maxGLSN   types.GLSN
	)
	grp, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < clus.NumberOfClients(); i++ {
		idx := i
		grp.Go(func() error {
			max := types.InvalidGLSN
			client := clus.ClientAtIndex(t, idx)
			for i := 0; i < numAppend; i++ {
				glsn, err := client.Append(ctx, []byte("foo"))
				if err != nil {
					return err
				}
				max = glsn
			}

			muMaxGLSN.Lock()
			defer muMaxGLSN.Unlock()
			if maxGLSN < max {
				maxGLSN = max
			}
			return nil
		})
	}
	require.NoError(t, grp.Wait())
	require.Equal(t, types.GLSN(numAppend*clus.NumberOfClients()), maxGLSN)

	subC := make(chan types.GLSN, maxGLSN)
	onNext := func(logEntry types.LogEntry, err error) {
		if err != nil {
			close(subC)
			return
		}
		subC <- logEntry.GLSN
	}

	client := clus.ClientAtIndex(t, rand.Intn(clus.NumberOfClients()))
	closer, err := client.Subscribe(context.Background(), types.MinGLSN, maxGLSN+1, onNext, varlog.SubscribeOption{})
	require.NoError(t, err)
	defer closer()

	expectedGLSN := types.MinGLSN
	for sub := range subC {
		require.Equal(t, expectedGLSN, sub)
		expectedGLSN++
	}
	require.Equal(t, expectedGLSN, maxGLSN+1)
}

func TestReadSealedLogStream(t *testing.T) {
	const boundary = types.GLSN(10)

	clus := it.NewVarlogCluster(t,
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(5),
	)
	defer clus.Close(t)

	// append logs
	var wg sync.WaitGroup
	errC := make(chan error, 1024)
	glsnC := make(chan types.GLSN, 1024)

	for i := 0; i < clus.NumberOfClients(); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			client := clus.ClientAtIndex(t, idx)
			for {
				glsn, err := client.Append(context.Background(), []byte("foo"))
				if err != nil {
					assert.ErrorIs(t, err, verrors.ErrSealed)
					errC <- err
					break
				}
				glsnC <- glsn
			}
		}(i)
	}

	// seal
	numSealedErr := 0
	sealedGLSN := types.InvalidGLSN
	lsID := clus.LogStreamIDs()[0]

	for numSealedErr < clus.NumberOfClients() {
		select {
		case glsn := <-glsnC:
			if sealedGLSN.Invalid() && glsn > boundary {
				rsp, err := clus.GetVMSClient(t).Seal(context.Background(), lsID)
				require.NoError(t, err)
				sealedGLSN = rsp.GetSealedGLSN()
				t.Logf("SealedGLSN: %v", sealedGLSN)
			}
		case err := <-errC:
			numSealedErr++
			require.ErrorIs(t, err, verrors.ErrSealed)
		}
	}

	wg.Wait()

	require.Greater(t, sealedGLSN, boundary)

	// TODO: Test if VMS keeps correct status for the log stream

	// read sealed log streams
	for glsn := types.MinGLSN; glsn <= sealedGLSN; glsn++ {
		idx := rand.Intn(clus.NumberOfClients())
		client := clus.ClientAtIndex(t, idx)
		_, err := client.Read(context.TODO(), lsID, glsn)
		require.NoError(t, err)
	}
}

func TestTrimGLS(t *testing.T) {
	opts := []it.Option{
		it.WithSnapCount(10),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(2),
		it.WithNumberOfClients(1),
		it.WithReporterClientFactory(metadata_repository.NewReporterClientFactory()),
		it.WithStorageNodeManagementClientFactory(metadata_repository.NewEmptyStorageNodeClientFactory()),
	}

	Convey("Given cluster, when a client appends logs", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		lsIDs := env.LogStreamIDs()
		client := env.ClientAtIndex(t, 0)

		var (
			err  error
			glsn types.GLSN
		)
		for i := 0; i < 10; i++ {
			glsn, err = client.AppendTo(context.Background(), lsIDs[0], []byte("foo"))
			So(err, ShouldBeNil)
			So(glsn, ShouldNotEqual, types.InvalidGLSN)
		}
		for i := 0; i < 10; i++ {
			glsn, err = client.AppendTo(context.Background(), lsIDs[1], []byte("foo"))
			So(err, ShouldBeNil)
			So(glsn, ShouldNotEqual, types.InvalidGLSN)
		}

		// TODO: Use RPC
		mr := env.GetMR(t)
		hwm := mr.GetHighWatermark()
		So(hwm, ShouldEqual, glsn)

		Convey("Then GLS history of MR should be trimmed", func(ctx C) {
			So(testutil.CompareWaitN(50, func() bool {
				return mr.GetMinHighWatermark() == mr.GetPrevHighWatermark()
			}), ShouldBeTrue)
		})
	}))
}

func TestTrimGLSWithSealedLS(t *testing.T) {
	Convey("Given cluster", t, func(ctx C) {
		opts := []it.Option{
			it.WithSnapCount(10),
			it.WithNumberOfStorageNodes(1),
			it.WithNumberOfLogStreams(2),
			it.WithNumberOfClients(1),
			it.WithReporterClientFactory(metadata_repository.NewReporterClientFactory()),
			it.WithStorageNodeManagementClientFactory(metadata_repository.NewEmptyStorageNodeClientFactory()),
		}

		Convey("When a client appends logs", it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
			lsIDs := env.LogStreamIDs()
			client := env.ClientAtIndex(t, 0)

			var err error
			glsn := types.InvalidGLSN
			for i := 0; i < 32; i++ {
				lsid := lsIDs[i%env.NumberOfLogStreams()]
				glsn, err = client.AppendTo(context.Background(), lsid, []byte("foo"))
				So(err, ShouldBeNil)
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			sealedLSID := lsIDs[0]
			runningLSID := lsIDs[1]

			_, err = env.GetVMSClient(t).Seal(context.Background(), sealedLSID)
			So(err, ShouldBeNil)

			for i := 0; i < 10; i++ {
				glsn, err = client.AppendTo(context.Background(), runningLSID, []byte("foo"))
				So(err, ShouldBeNil)
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			// TODO: Use RPC
			mr := env.GetMR(t)
			hwm := mr.GetHighWatermark()
			So(hwm, ShouldEqual, glsn)

			Convey("Then GLS history of MR should be trimmed", func(ctx C) {
				So(testutil.CompareWaitN(50, func() bool {
					return mr.GetMinHighWatermark() == mr.GetPrevHighWatermark()
				}), ShouldBeTrue)
			})
		}))
	})
}
