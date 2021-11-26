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

	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/tests/it"
)

func TestAppendLogs(t *testing.T) {
	const numAppend = 100

	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(10),
		it.WithNumberOfClients(10),
		it.WithNumberOfTopics(1),
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
			topicID := clus.TopicIDs()[0]
			client := clus.ClientAtIndex(t, idx)
			for i := 0; i < numAppend; i++ {
				lem, err := client.Append(ctx, topicID, []byte("foo"))
				if err != nil {
					return err
				}
				max = lem.GLSN
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
	onNext := func(logEntry varlogpb.LogEntry, err error) {
		if err != nil {
			close(subC)
			return
		}
		subC <- logEntry.GLSN
	}

	topicID := clus.TopicIDs()[0]
	client := clus.ClientAtIndex(t, rand.Intn(clus.NumberOfClients()))
	closer, err := client.Subscribe(context.Background(), topicID, types.MinGLSN, maxGLSN+1, onNext)
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
		it.WithNumberOfTopics(1),
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
			topicID := clus.TopicIDs()[0]
			client := clus.ClientAtIndex(t, idx)
			for {
				lem, err := client.Append(context.Background(), topicID, []byte("foo"))
				if err != nil {
					assert.ErrorIs(t, err, verrors.ErrSealed)
					errC <- err
					break
				}
				glsnC <- lem.GLSN
			}
		}(i)
	}

	// seal
	numSealedErr := 0
	sealedGLSN := types.InvalidGLSN
	topicID := clus.TopicIDs()[0]
	lsID := clus.LogStreamIDs(topicID)[0]

	for numSealedErr < clus.NumberOfClients() {
		select {
		case glsn := <-glsnC:
			if sealedGLSN.Invalid() && glsn > boundary {
				rsp, err := clus.GetVMSClient(t).Seal(context.Background(), topicID, lsID)
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
		_, err := client.Read(context.TODO(), topicID, lsID, glsn)
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
		it.WithNumberOfTopics(1),
	}

	Convey("Given cluster, when a client appends logs", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		topicID := env.TopicIDs()[0]
		lsIDs := env.LogStreamIDs(topicID)
		client := env.ClientAtIndex(t, 0)

		var glsn types.GLSN
		for i := 0; i < 10; i++ {
			lem, err := client.AppendTo(context.Background(), topicID, lsIDs[0], []byte("foo"))
			So(err, ShouldBeNil)
			glsn = lem.GLSN
			So(glsn, ShouldNotEqual, types.InvalidGLSN)
		}
		for i := 0; i < 10; i++ {
			lem, err := client.AppendTo(context.Background(), topicID, lsIDs[1], []byte("foo"))
			So(err, ShouldBeNil)
			glsn = lem.GLSN
			So(glsn, ShouldNotEqual, types.InvalidGLSN)
		}

		// TODO: Use RPC
		mr := env.GetMR(t)
		ver := mr.GetLastCommitVersion()
		So(ver, ShouldEqual, types.Version(glsn))

		Convey("Then GLS history of MR should be trimmed", func(ctx C) {
			So(testutil.CompareWaitN(50, func() bool {
				return mr.GetOldestCommitVersion() == mr.GetLastCommitVersion()-1
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
			it.WithNumberOfTopics(1),
		}

		Convey("When a client appends logs", it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
			topicID := env.TopicIDs()[0]
			lsIDs := env.LogStreamIDs(topicID)
			client := env.ClientAtIndex(t, 0)

			var err error
			glsn := types.InvalidGLSN
			for i := 0; i < 32; i++ {
				lsid := lsIDs[i%env.NumberOfLogStreams(topicID)]
				lem, err := client.AppendTo(context.Background(), topicID, lsid, []byte("foo"))
				So(err, ShouldBeNil)
				glsn = lem.GLSN
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			sealedLSID := lsIDs[0]
			runningLSID := lsIDs[1]

			_, err = env.GetVMSClient(t).Seal(context.Background(), topicID, sealedLSID)
			So(err, ShouldBeNil)

			for i := 0; i < 10; i++ {
				lem, err := client.AppendTo(context.Background(), topicID, runningLSID, []byte("foo"))
				So(err, ShouldBeNil)
				glsn = lem.GLSN
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			// TODO: Use RPC
			mr := env.GetMR(t)
			ver := mr.GetLastCommitVersion()
			So(ver, ShouldEqual, types.Version(glsn))

			Convey("Then GLS history of MR should be trimmed", func(ctx C) {
				So(testutil.CompareWaitN(50, func() bool {
					return mr.GetOldestCommitVersion() == ver-1
				}), ShouldBeTrue)
			})
		}))
	})
}

func TestNewbieLogStream(t *testing.T) {
	opts := []it.Option{
		it.WithSnapCount(10),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(2),
		it.WithNumberOfClients(1),
		it.WithReporterClientFactory(metadata_repository.NewReporterClientFactory()),
		it.WithStorageNodeManagementClientFactory(metadata_repository.NewEmptyStorageNodeClientFactory()),
		it.WithNumberOfTopics(1),
	}

	Convey("Given LogStream", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		topicID := env.TopicIDs()[0]
		lsIDs := env.LogStreamIDs(topicID)
		client := env.ClientAtIndex(t, 0)

		for i := 0; i < 32; i++ {
			lsid := lsIDs[i%env.NumberOfLogStreams(topicID)]
			lem, err := client.AppendTo(context.Background(), topicID, lsid, []byte("foo"))
			So(err, ShouldBeNil)
			So(lem.GLSN, ShouldNotEqual, types.InvalidGLSN)
		}

		Convey("When add new logStream", func(ctx C) {
			env.AddLS(t, topicID)
			env.ClientRefresh(t)

			Convey("Then it should be appendable", func(ctx C) {
				topicID := env.TopicIDs()[0]
				lsIDs := env.LogStreamIDs(topicID)
				client := env.ClientAtIndex(t, 0)

				for i := 0; i < 32; i++ {
					lsid := lsIDs[i%env.NumberOfLogStreams(topicID)]
					lem, err := client.AppendTo(context.Background(), topicID, lsid, []byte("foo"))
					So(err, ShouldBeNil)
					So(lem.GLSN, ShouldNotEqual, types.InvalidGLSN)
				}
			})
		})
	}))
}
