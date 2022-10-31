package metarepos

import (
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/kakao/varlog/tests/ee/cluster/local/daemon"
)

// Node represents a metadata repository node.
type Node struct {
	config

	mu      sync.Mutex
	wg      sync.WaitGroup
	proc    *daemon.Daemon
	running bool
}

// New creates a new metadata repository node.
func New(t *testing.T, opts ...Option) *Node {
	cfg, err := newConfig(opts)
	require.NoError(t, err)
	return &Node{
		config: cfg,
	}
}

// ResetPeers updates peer nodes for Raft.
func (node *Node) ResetPeers(peers []string) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.peers = peers
}

// Start runs a metadata repository node.
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
	node.logger.Info("started metadata repository node", zap.String("command", p.String()))
}

// Stop terminates a metadata repository node.
func (node *Node) Stop(t *testing.T) {
	node.mu.Lock()
	defer node.mu.Unlock()

	require.True(t, node.running)
	node.proc.Stop()
	node.wg.Wait()
	node.running = false
	node.logger.Info("stopped metadata repository node")
}

// Running returns true if a metadata repository is running.
func (node *Node) Running() bool {
	node.mu.Lock()
	defer node.mu.Unlock()
	return node.running
}

// RPCAddress returns an address for RPC communication.
func (node *Node) RPCAddress() string {
	return node.rpcAddr
}

// RaftURL returns an URL for Raft.
func (node *Node) RaftURL() string {
	return node.raftURL
}

func (node *Node) arguments(t *testing.T) []string {
	args := []string{
		"start",
		"--cluster-id", node.cid.String(),
		"--raft-address", node.raftURL,
		"--bind", node.rpcAddr,
		"--log-rep-factor", strconv.Itoa(node.replicationFactor),
		"--raft-dir", node.raftDir,
		"--log-dir", node.logDir,
	}

	wals, err := filepath.Glob(filepath.Join(node.raftDir, "wal", "*", "*.wal"))
	require.NoError(t, err)
	restart := len(wals) > 0

	if len(node.peers) > 0 || restart {
		args = append(args, "--join")
	}

	if !restart {
		for _, peer := range node.peers {
			args = append(args, "--peers", peer)
		}
	}

	return args
}
