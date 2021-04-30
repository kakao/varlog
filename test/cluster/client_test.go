package cluster

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/internal/vms"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/test"
)

func testCluster(t *testing.T) (*test.VarlogCluster, func()) {
	vmsOpts := vms.DefaultOptions()
	vmsOpts.Tick = 100 * time.Millisecond
	vmsOpts.HeartbeatTimeout = 30
	vmsOpts.ReportInterval = 10
	vmsOpts.Logger = zap.L()

	opts := test.VarlogClusterOptions{
		NrMR:                  1,
		NrRep:                 3,
		ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
		SNManagementClientFac: metadata_repository.NewStorageNodeManagementClientFactory(),
		VMSOpts:               &vmsOpts,
	}

	clus := test.NewVarlogCluster(opts)
	clus.Start()

	// MR HealthCheck
	require.Eventually(t, clus.HealthCheck, 10*time.Second, time.Second)
	require.Eventually(t, func() bool {
		return clus.GetMR().GetServerAddr() != ""
	}, time.Second, 10*time.Millisecond)

	// VMS
	_, err := clus.RunClusterManager([]string{clus.GetMR().GetServerAddr()}, opts.VMSOpts)
	require.NoError(t, err)

	_, err = clus.NewClusterManagerClient()
	require.NoError(t, err)

	closer := func() {
		require.NoError(t, clus.CMCli.Close())
		require.NoError(t, clus.Close())
		testutil.GC()
	}

	return clus, closer
}

func testStartSN(t *testing.T, clus *test.VarlogCluster, num int) {
	for i := 0; i < num; i++ {
		_, err := clus.AddSNByVMS()
		require.NoError(t, err)
	}

	// TODO: move this assertions to integration testing framework
	rsp, err := clus.CMCli.GetStorageNodes(context.TODO())
	require.NoError(t, err)
	require.Len(t, rsp.GetStoragenodes(), num)

	t.Logf("Started %d SNs", num)
}

func testAddLS(t *testing.T, clus *test.VarlogCluster, num int) {
	for i := 0; i < num; i++ {
		_, err := clus.AddLSByVMS()
		require.NoError(t, err)
	}

	t.Logf("Started %d LSs", num)
}

func TestClientNoLogStream(t *testing.T) {
	// FIXME: detected some leaked goroutines:
	// go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop(0xc0000b0d08)
	// vendor/go.etcd.io/etcd/pkg/logutil/merge_logger.go:173 +0x4bb
	//
	// defer goleak.VerifyNone(t)

	const nrSN = 3

	clus, closer := testCluster(t)
	defer closer()

	testStartSN(t, clus, nrSN)

	vcl, err := varlog.Open(
		context.TODO(),
		clus.ClusterID,
		[]string{clus.GetMR().GetServerAddr()},
		varlog.WithLogger(zap.L()),
		varlog.WithOpenTimeout(3*time.Second),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, vcl.Close())
	}()

	_, err = vcl.Append(context.TODO(), []byte("foo"))
	require.Error(t, err)
}

func TestClientAppendTo(t *testing.T) {
	// defer goleak.VerifyNone(t)

	const (
		nrSN = 3
		nrLS = 3
	)

	clus, closer := testCluster(t)
	defer closer()

	testStartSN(t, clus, nrSN)
	testAddLS(t, clus, nrLS)

	vcl, err := varlog.Open(
		context.TODO(),
		clus.ClusterID,
		[]string{clus.GetMR().GetServerAddr()},
		varlog.WithLogger(zap.L()),
		varlog.WithOpenTimeout(3*time.Second),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, vcl.Close())
	}()

	logStreamIDs := clus.IssuedLogStreams()

	_, err = vcl.AppendTo(context.TODO(), logStreamIDs[len(logStreamIDs)-1]+1, []byte("foo"))
	require.Error(t, err)

	_, err = vcl.AppendTo(context.TODO(), logStreamIDs[0], []byte("foo"))
	require.NoError(t, err)
}

func TestClientAppend(t *testing.T) {
	// defer goleak.VerifyNone(t)

	const (
		nrSN   = 3
		nrLS   = 3
		nrLogs = 10
	)

	clus, closer := testCluster(t)
	defer closer()

	testStartSN(t, clus, nrSN)
	testAddLS(t, clus, nrLS)

	vcl, err := varlog.Open(
		context.TODO(),
		clus.ClusterID,
		[]string{clus.GetMR().GetServerAddr()},
		varlog.WithLogger(zap.L()),
		varlog.WithOpenTimeout(3*time.Second),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, vcl.Close())
	}()

	for i := 0; i < nrLogs; i++ {
		_, err := vcl.Append(context.TODO(), []byte("foo"))
		require.NoError(t, err)
	}

	require.Condition(t, func() bool {
		for _, lsid := range clus.IssuedLogStreams() {
			if _, errRead := vcl.Read(context.TODO(), lsid, 1); errRead == nil {
				return true
			}
		}
		return false
	})
}

func TestClientSubscribe(t *testing.T) {
	// defer goleak.VerifyNone(t)

	const (
		nrSN   = 3
		nrLS   = 3
		nrLogs = 10
	)

	clus, closer := testCluster(t)
	defer closer()

	testStartSN(t, clus, nrSN)
	testAddLS(t, clus, nrLS)

	vcl, err := varlog.Open(
		context.TODO(),
		clus.ClusterID,
		[]string{clus.GetMR().GetServerAddr()},
		varlog.WithLogger(zap.L()),
		varlog.WithOpenTimeout(3*time.Second),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, vcl.Close())
	}()

	for i := 0; i < nrLogs; i++ {
		_, err := vcl.Append(context.TODO(), []byte("foo"))
		require.NoError(t, err)
	}

	errc := make(chan error, nrLogs)
	expectedGLSN := types.GLSN(1)
	subscribeCloser, err := vcl.Subscribe(context.TODO(), types.GLSN(1), types.GLSN(nrLogs+1), func(le types.LogEntry, err error) {
		if err != nil {
			require.ErrorIs(t, io.EOF, err)
			defer close(errc)
			return
		}
		assert.Equal(t, expectedGLSN, le.GLSN)
		expectedGLSN++
		errc <- err
	}, varlog.SubscribeOption{})
	require.NoError(t, err)
	defer subscribeCloser()

	for e := range errc {
		if e != nil {
			require.ErrorIs(t, io.EOF, e)
		}
	}
}
