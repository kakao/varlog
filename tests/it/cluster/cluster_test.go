package cluster

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/internal/metarepos"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/tests/it"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreTopFunction(
		"go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop",
	))
}

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
				res := client.Append(ctx, topicID, [][]byte{[]byte("foo")})
				if res.Err != nil {
					return res.Err
				}
				max = res.Metadata[0].GLSN
			}
			t.Logf("[%d] Append completed", idx)

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
				t.Logf("[%d] appending", idx)
				res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
				if res.Err != nil {
					t.Logf("[%d] append error: %+v", idx, res.Err)
					assert.ErrorIs(t, res.Err, verrors.ErrSealed)
					errC <- res.Err
					break
				}
				glsnC <- res.Metadata[0].GLSN
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
			t.Logf("%d sealed errors", numSealedErr)
			require.ErrorIs(t, err, verrors.ErrSealed)
		}
	}

	wg.Wait()

	require.Greater(t, sealedGLSN, boundary)

	// TODO: Test if VMS keeps correct status for the log stream

	// NOTE: Read API is deprecated.
	// read sealed log streams
	// for glsn := types.MinGLSN; glsn <= sealedGLSN; glsn++ {
	//	idx := rand.Intn(clus.NumberOfClients())
	//	client := clus.ClientAtIndex(t, idx)
	//	_, err := client.Read(context.TODO(), topicID, lsID, glsn)
	//	require.NoError(t, err)
	// }
}

func TestTrimGLS(t *testing.T) {
	opts := []it.Option{
		it.WithSnapCount(10),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(2),
		it.WithNumberOfClients(1),
		it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		it.WithNumberOfTopics(1),
	}

	Convey("Given cluster, when a client appends logs", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		topicID := env.TopicIDs()[0]
		lsIDs := env.LogStreamIDs(topicID)
		client := env.ClientAtIndex(t, 0)

		var glsn types.GLSN
		for i := 0; i < 10; i++ {
			res := client.AppendTo(context.Background(), topicID, lsIDs[0], [][]byte{[]byte("foo")})
			So(res.Err, ShouldBeNil)
			glsn = res.Metadata[0].GLSN
			So(glsn, ShouldNotEqual, types.InvalidGLSN)
		}
		for i := 0; i < 10; i++ {
			res := client.AppendTo(context.Background(), topicID, lsIDs[1], [][]byte{[]byte("foo")})
			So(res.Err, ShouldBeNil)
			glsn = res.Metadata[0].GLSN
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
			it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
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
				res := client.AppendTo(context.Background(), topicID, lsid, [][]byte{[]byte("foo")})
				So(res.Err, ShouldBeNil)
				glsn = res.Metadata[0].GLSN
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			sealedLSID := lsIDs[0]
			runningLSID := lsIDs[1]

			_, err = env.GetVMSClient(t).Seal(context.Background(), topicID, sealedLSID)
			So(err, ShouldBeNil)

			for i := 0; i < 10; i++ {
				res := client.AppendTo(context.Background(), topicID, runningLSID, [][]byte{[]byte("foo")})
				So(res.Err, ShouldBeNil)
				glsn = res.Metadata[0].GLSN
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
		it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		it.WithNumberOfTopics(1),
	}

	Convey("Given LogStream", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		topicID := env.TopicIDs()[0]
		lsIDs := env.LogStreamIDs(topicID)
		client := env.ClientAtIndex(t, 0)

		for i := 0; i < 32; i++ {
			lsid := lsIDs[i%env.NumberOfLogStreams(topicID)]
			res := client.AppendTo(context.Background(), topicID, lsid, [][]byte{[]byte("foo")})
			So(res.Err, ShouldBeNil)
			So(res.Metadata[0].GLSN, ShouldNotEqual, types.InvalidGLSN)
		}

		Convey("When add new logStream", func(ctx C) {
			env.AddLS(t, topicID)
			env.ClientRefresh(t)

			Convey("Then it should be appendable", func(ctx C) {
				topicID := env.TopicIDs()[0]
				lsIDs := env.LogStreamIDs(topicID)
				client := env.ClientAtIndex(t, 0)

				for i := 0; i < 32; i++ {
					// AppendTo Newbie first
					numLS := env.NumberOfLogStreams(topicID)
					lsid := lsIDs[(numLS-1)-(i%numLS)]
					res := client.AppendTo(context.Background(), topicID, lsid, [][]byte{[]byte("foo")})
					So(res.Err, ShouldBeNil)
					So(res.Metadata[0].GLSN, ShouldNotEqual, types.InvalidGLSN)
				}
			})
		})
	}))
}
