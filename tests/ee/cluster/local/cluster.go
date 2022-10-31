package local

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/tests/ee/cluster"
	"github.com/kakao/varlog/tests/ee/cluster/local/daemon"
	"github.com/kakao/varlog/tests/ee/cluster/local/metarepos"
	"github.com/kakao/varlog/tests/ee/cluster/local/storagenode"
	"github.com/kakao/varlog/tests/ee/controller"
	"github.com/kakao/varlog/tests/ee/grpc"
)

type Cluster struct {
	config

	ctrl *controller.Controller

	metadataRepository struct {
		mu    sync.Mutex
		nodes []*metarepos.Node
		peers []string
	}

	storageNodes struct {
		mu    sync.Mutex
		nodes []*storagenode.Node
	}

	adminServer struct {
		mu   sync.Mutex
		node *daemon.Daemon
		wg   sync.WaitGroup
	}
}

var _ cluster.Cluster = (*Cluster)(nil)

func New(t *testing.T, opts ...Option) *Cluster {
	cfg, err := newConfig(opts)
	require.NoError(t, err)
	return &Cluster{
		config: cfg,
		ctrl:   controller.New(cfg.adminAddr, cfg.Logger()),
	}
}

func (c *Cluster) Setup(_ context.Context, t *testing.T) {
	var ctx context.Context
	c.StartMetadataRepositoryNodes(ctx, t, 1)
	c.StartAdminServer(ctx, t)
	if remains := c.NumMetaRepos() - 1; remains > 0 {
		c.StartMetadataRepositoryNodes(ctx, t, remains)
	}
	if desired := c.NumStorageNodes(); desired > 0 {
		c.StartStorageNodes(ctx, t, desired)
	}
}

func (c *Cluster) createAdminServer(t *testing.T) {
	mrAddr := c.MetadataRepositoryAddress(context.Background(), t)
	p, err := daemon.New(
		c.adminExecutable,
		daemon.WithArguments(
			"start",
			"--cluster-id", c.ClusterID().String(),
			"--listen", c.adminAddr,
			"--replication-factor", strconv.Itoa(c.ReplicationFactor()),
			"--metadata-repository", mrAddr,
			"--logtostderr",
		),
	)
	require.NoError(t, err)
	c.adminServer.node = p
}

func (c *Cluster) StartAdminServer(_ context.Context, t *testing.T) {
	c.createAdminServer(t)

	c.adminServer.wg.Add(1)
	go func() {
		defer c.adminServer.wg.Done()
		_ = c.adminServer.node.Run()
	}()

	c.Logger().Info("started admin server", zap.String("command", c.adminServer.node.String()))
	grpc.HealthProbe(t, c.grpcHealthProbeExecutable, c.adminAddr)
}

func (c *Cluster) StopAdminServer(_ context.Context, t *testing.T) {
	c.adminServer.node.Stop()
	c.adminServer.wg.Wait()
}

func (c *Cluster) AdminServerAddress(context.Context, *testing.T) string {
	return c.adminAddr
}

func (c *Cluster) StartMetadataRepositoryNodes(ctx context.Context, t *testing.T, desired int) {
	c.SetNumMetadataRepositories(ctx, t, desired)
}

func (c *Cluster) StopMetadataRepositoryNodes(ctx context.Context, t *testing.T) {
	c.SetNumMetadataRepositories(ctx, t, 0)
}

func (c *Cluster) SetNumMetadataRepositories(_ context.Context, t *testing.T, desired int) {
	require.GreaterOrEqual(t, desired, 0)

	c.metadataRepository.mu.Lock()
	defer c.metadataRepository.mu.Unlock()

	var running []*metarepos.Node
	var stopped []*metarepos.Node
	for _, node := range c.metadataRepository.nodes {
		if node.Running() {
			running = append(running, node)
			continue
		}
		stopped = append(stopped, node)
	}

	need := desired - len(running)
	if need == 0 {
		return
	}
	if need < 0 { // stop nodes
		for i := 0; i < -need; i++ {
			running[i].Stop(t)
		}
		return
	}

	// start nodes again
	for _, node := range stopped {
		if need == 0 {
			return
		}
		node.Start(t)
		grpc.HealthProbe(t, c.grpcHealthProbeExecutable, node.RPCAddress())
		need--
	}
	for i := 0; i < need; i++ {
		// more start new node
		n := len(c.metadataRepository.nodes)
		nodeName := "mr-" + strconv.Itoa(n+1)
		raftPort := c.mrPortBase + (n * 2)
		rpcPort := c.mrPortBase + (n*2 + 1)
		raftURL := "http://127.0.0.1:" + strconv.Itoa(raftPort)
		rpcAddr := "127.0.0.1:" + strconv.Itoa(rpcPort)
		opts := []metarepos.Option{
			metarepos.WithExecutable(c.mrExecutable),
			metarepos.WithNodeName(nodeName),
			metarepos.WithClusterID(c.ClusterID()),
			metarepos.WithReplicationFactor(c.ReplicationFactor()),
			metarepos.WithRaftURL(raftURL),
			metarepos.WithRPCAddr(rpcAddr),
			metarepos.WithRaftDir(t.TempDir()),
			metarepos.WithLogDir(t.TempDir()),
			metarepos.WithPeers(c.metadataRepository.peers),
			metarepos.WithLogger(c.Logger()),
		}
		mrNode := metarepos.New(t, opts...)
		mrNode.Start(t)

		if len(c.metadataRepository.peers) > 0 {
			c.ctrl.AddRaftPeer(t, raftURL, rpcAddr)
		}

		grpc.HealthProbe(t, c.grpcHealthProbeExecutable, rpcAddr)

		c.metadataRepository.nodes = append(c.metadataRepository.nodes, mrNode)
		c.metadataRepository.peers = append(c.metadataRepository.peers, raftURL)
	}
}

func (c *Cluster) MetadataRepositoryAddress(_ context.Context, t *testing.T) string {
	c.metadataRepository.mu.Lock()
	defer c.metadataRepository.mu.Unlock()

	for _, mr := range c.metadataRepository.nodes {
		if mr.Running() {
			return mr.RPCAddress()
		}
	}
	require.Fail(t, "no available metadata repository node")
	return ""
}

func (c *Cluster) StartStorageNodes(_ context.Context, t *testing.T, desired int) {
	c.storageNodes.mu.Lock()
	defer c.storageNodes.mu.Unlock()

	var stopped []*storagenode.Node
	running := 0
	for _, node := range c.storageNodes.nodes {
		if node.Running() {
			running++
			continue
		}
		stopped = append(stopped, node)
	}
	require.Less(t, running, desired)

	need := desired - running
	for _, node := range stopped {
		if need <= 0 {
			break
		}
		node.Start(t)
		grpc.HealthProbe(t, c.grpcHealthProbeExecutable, node.Address())
		need--
	}

	for i := 0; i < need; i++ {
		snid := types.StorageNodeID(len(c.storageNodes.nodes) + 1)
		nodeName := "sn-" + snid.String()
		port := c.snPortBase + len(c.storageNodes.nodes)
		addr := "127.0.0.1:" + strconv.Itoa(port)
		node := storagenode.New(t,
			storagenode.WithExecutable(c.snExecutable),
			storagenode.WithNodeName(nodeName),
			storagenode.WithClusterID(c.ClusterID()),
			storagenode.WithStorageNodeID(snid),
			storagenode.WithAddress(addr),
			storagenode.WithVolumes(t.TempDir()),
			storagenode.WithLogger(c.Logger()),
		)
		node.Start(t)
		grpc.HealthProbe(t, c.grpcHealthProbeExecutable, node.Address())
		c.ctrl.AddStorageNode(t, snid, addr)
		c.storageNodes.nodes = append(c.storageNodes.nodes, node)
	}
}

func (c *Cluster) StopStorageNodes(_ context.Context, t *testing.T) {
	c.storageNodes.mu.Lock()
	defer c.storageNodes.mu.Unlock()

	for _, node := range c.storageNodes.nodes {
		node.Stop(t)
	}
}

func (c *Cluster) StartStorageNode(_ context.Context, t *testing.T, nodeName string) bool {
	c.storageNodes.mu.Lock()
	defer c.storageNodes.mu.Unlock()

	for _, node := range c.storageNodes.nodes {
		if node.Name() == nodeName {
			require.False(t, node.Running())
			node.Start(t)
			grpc.HealthProbe(t, c.grpcHealthProbeExecutable, node.Address())
			return true
		}
	}
	require.Fail(t, "start storage node: no such node", zap.String("node", nodeName))
	return false
}

func (c *Cluster) StopStorageNode(_ context.Context, t *testing.T, snid types.StorageNodeID) (nodeName string) {
	c.storageNodes.mu.Lock()
	defer c.storageNodes.mu.Unlock()

	for _, node := range c.storageNodes.nodes {
		if node.StorageNodeID() == snid {
			require.True(t, node.Running())
			node.Stop(t)
			return node.Name()
		}
	}
	require.Fail(t, "stop storage node: no such id", zap.Any("snid", snid))
	return ""
}

func (c *Cluster) GetStorageNodeByNodeName(t *testing.T, nodeName string) *storagenode.Node {
	c.storageNodes.mu.Lock()
	defer c.storageNodes.mu.Unlock()

	for _, node := range c.storageNodes.nodes {
		if node.Name() == nodeName {
			return node
		}
	}
	require.Fail(t, "no such storage node", zap.String("node", nodeName))
	return nil
}

func (c *Cluster) Controller() *controller.Controller {
	return c.ctrl
}

func (c *Cluster) String() string {
	return "local"
}

func (c *Cluster) Close(ctx context.Context, t *testing.T) {
	c.StopAdminServer(ctx, t)
	c.StopMetadataRepositoryNodes(ctx, t)
	c.StopStorageNodes(ctx, t)
}
