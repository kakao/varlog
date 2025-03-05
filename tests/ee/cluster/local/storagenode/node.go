package storagenode

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/tests/ee/cluster/local/daemon"
)

type Node struct {
	config

	mu      sync.Mutex
	wg      sync.WaitGroup
	proc    *daemon.Daemon
	running bool
}

func New(t *testing.T, opts ...Option) *Node {
	cfg, err := newConfig(opts)
	require.NoError(t, err)
	return &Node{
		config: cfg,
	}
}

func (node *Node) Start(t *testing.T) {
	node.mu.Lock()
	defer node.mu.Unlock()

	require.False(t, node.running)
	args := node.arguments(t)
	p, err := daemon.New(
		node.executable,
		daemon.WithArguments(args...),
	)
	require.NoError(t, err)

	node.wg.Add(1)
	go func() {
		defer node.wg.Done()
		_ = p.Run()
	}()
	node.proc = p
	node.running = true
	node.logger.Info("started storage node", zap.String("command", p.String()))
}

func (node *Node) Stop(t *testing.T) {
	node.mu.Lock()
	defer node.mu.Unlock()

	require.True(t, node.running)
	node.proc.Stop()
	node.wg.Wait()
	node.running = false
	node.logger.Info("stopped storage node")
}

func (node *Node) Running() bool {
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.running
}

func (node *Node) Address() string {
	return node.addr
}

func (node *Node) StorageNodeID() types.StorageNodeID {
	return node.snid
}

func (node *Node) Name() string {
	return node.name
}

func (node *Node) arguments(t *testing.T) []string {
	args := []string{
		"start",
		"--cluster-id", node.cid.String(),
		"--snid", node.snid.String(),
		"--listen", node.addr,
		"--advertise", node.addr,
		"--logtostderr",
		"--storage-datadb-no-sync",
	}
	for _, volume := range node.volumes {
		args = append(args, "--volumes", volume)
	}
	return args
}
