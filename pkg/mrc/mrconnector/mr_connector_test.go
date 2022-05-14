package mrconnector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kakao/varlog/internal/metarepos"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil/ports"
	"github.com/kakao/varlog/vtesting"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreTopFunction(
		"go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop",
	))
}

func TestConnectorBadOptions(t *testing.T) {
	var err error

	_, err = New(context.Background(),
		WithLogger(nil),
		WithSeed([]string{"127.0.0.1:1"}),
	)
	require.Error(t, err)

	_, err = New(context.Background(), WithSeed(nil))
	require.Error(t, err)

	_, err = New(context.Background(), WithSeed([]string{}))
	require.Error(t, err)

	_, err = New(context.Background(),
		WithInitCount(0),
		WithSeed([]string{"127.0.0.1:1"}),
	)
	require.Error(t, err)
}

func TestConnectorWithoutLiveMR(t *testing.T) {
	_, err := New(
		context.Background(),
		WithSeed([]string{
			"127.0.0.1:1",
			"127.0.0.1:2",
			"127.0.0.1:3",
		}),
	)
	require.Error(t, err)
}

type testMR struct {
	raftAddr  string
	rpcAddr   string
	clusterID types.ClusterID
	nodeID    types.NodeID
	mr        *metarepos.RaftMetadataRepository
}

func newTestMR(t *testing.T, portLease *ports.Lease, clusterID types.ClusterID, cnt int) []testMR {
	raftAddrs := make([]string, 0, cnt)
	testMRs := make([]testMR, 0, cnt)

	for i := 0; i < cnt; i++ {
		raftPort := portLease.Base() + i*2
		rpcPort := raftPort + i*2 + 1
		raftAddr := fmt.Sprintf("http://127.0.0.1:%d", raftPort)
		rpcAddr := fmt.Sprintf("127.0.0.1:%d", rpcPort)
		testMRs = append(testMRs, testMR{
			raftAddr: raftAddr,
			rpcAddr:  rpcAddr,
		})
		raftAddrs = append(raftAddrs, raftAddr)
	}

	for i := 0; i < cnt; i++ {
		raftAddr := testMRs[i].raftAddr
		rpcAddr := testMRs[i].rpcAddr
		raftDir := t.TempDir()

		nodeID := types.NewNodeIDFromURL(raftAddr)
		require.NotEqual(t, types.InvalidNodeID, nodeID)

		testMRs[i].clusterID = clusterID
		testMRs[i].nodeID = nodeID

		opts := &metarepos.MetadataRepositoryOptions{
			RaftOptions: metarepos.RaftOptions{
				Join:        false,
				UnsafeNoWal: false,
				SnapCount:   10,
				RaftTick:    vtesting.TestRaftTick(),
				RaftDir:     raftDir,
				Peers:       raftAddrs,
			},
			ClusterID:         clusterID,
			RaftAddress:       raftAddr,
			RPCTimeout:        time.Second,
			NumRep:            1,
			RPCBindAddress:    rpcAddr,
			ReporterClientFac: metarepos.NewReporterClientFactory(),
			Logger:            zap.NewNop(),
		}
		testMRs[i].mr = metarepos.NewRaftMetadataRepository(opts)
	}

	for i := 0; i < cnt; i++ {
		testMRs[i].mr.Run()
	}

	for i := 0; i < cnt; i++ {
		rpcAddr := testMRs[i].rpcAddr
		require.Eventually(t, func() bool {
			conn, err := rpc.NewConn(context.Background(), rpcAddr)
			if !assert.NoError(t, err) {
				return false
			}
			defer func() {
				_ = conn.Close()
			}()

			rsp, err := grpc_health_v1.NewHealthClient(conn.Conn).Check(
				context.Background(), &grpc_health_v1.HealthCheckRequest{},
			)
			return err == nil && rsp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING
		}, 10*time.Second, 100*time.Millisecond)
	}

	return testMRs
}

func TestConnectorUnreachableMR(t *testing.T) {
	const (
		basePort      = 10000
		fetchInterval = 100 * time.Millisecond
	)

	portLease, err := ports.ReserveWeaklyWithRetry(basePort)
	require.NoError(t, err)
	defer portLease.Release()

	mrs := newTestMR(t, portLease, 1, 1)

	// seed addresses
	seed := make([]string, 0, 1)
	for _, mr := range mrs {
		seed = append(seed, mr.rpcAddr)
	}

	connector, err := New(
		context.Background(),
		WithSeed(seed),
		WithInitRetryInterval(fetchInterval),
		WithUpdateInterval(fetchInterval),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, connector.Close())
	}()
	require.Equal(t, mrs[0].nodeID, connector.ConnectedNodeID())

	for _, mr := range mrs {
		require.NoError(t, mr.mr.Close())
	}

	require.Eventually(t, func() bool {
		return connector.ConnectedNodeID() == types.InvalidNodeID
	}, fetchInterval*100, fetchInterval)
}

func TestConnectorRemovePeer(t *testing.T) {
	const (
		numMRs        = 2
		clusterID     = types.ClusterID(1)
		basePort      = 10000
		fetchInterval = 100 * time.Millisecond
	)

	portLease, err := ports.ReserveWeaklyWithRetry(basePort)
	require.NoError(t, err)
	defer portLease.Release()

	mrs := newTestMR(t, portLease, clusterID, numMRs)
	defer func() {
		for _, mr := range mrs {
			require.NoError(t, mr.mr.Close())
		}
	}()

	// seed addresses
	seed := make([]string, 0, numMRs)
	for _, mr := range mrs {
		seed = append(seed, mr.rpcAddr)
	}

	connector, err := New(
		context.Background(),
		WithSeed(seed),
		WithInitCount(100),
		WithInitRetryInterval(fetchInterval),
		WithUpdateInterval(fetchInterval),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, connector.Close())
	}()

	require.Eventually(t, func() bool {
		for _, mr := range mrs {
			if !mr.mr.IsMember() {
				return false
			}

			info, err := mr.mr.GetClusterInfo(context.Background(), clusterID)
			if err != nil {
				return false
			}
			if len(info.Members) != numMRs {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		connectedNodeID := connector.ConnectedNodeID()
		return len(connector.ActiveMRs()) == numMRs && connectedNodeID != types.InvalidNodeID
	}, time.Minute, time.Second)

	require.Eventually(t, func() bool {
		mcl, err := connector.ManagementClient(context.Background())
		if err != nil {
			_ = mcl.Close()
			return false
		}
		return mcl.RemovePeer(context.Background(), clusterID, mrs[0].nodeID) == nil
	}, time.Minute, time.Second)

	require.Eventually(t, func() bool {
		return len(connector.ActiveMRs()) == 1
	}, time.Minute, time.Second)
}
