package metarepos

import (
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

type config struct {
	executable        string
	name              string
	cid               types.ClusterID
	replicationFactor int
	raftURL           string
	rpcAddr           string
	raftDir           string
	logDir            string
	peers             []string
	logger            *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg, nil
}

// Option configures a Node for metadata repository.
type Option interface {
	apply(*config)
}

type funcOption struct {
	f func(*config)
}

func newFuncOption(f func(*config)) *funcOption {
	return &funcOption{f: f}
}

func (fo *funcOption) apply(cfg *config) {
	fo.f(cfg)
}

// WithExecutable sets an executable for the metadata repository.
func WithExecutable(executable string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.executable = executable
	})
}

// WithNodeName sets a node name. Users should select a unique name.
func WithNodeName(name string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.name = name
	})
}

// WithClusterID sets a cluster ID.
func WithClusterID(cid types.ClusterID) Option {
	return newFuncOption(func(cfg *config) {
		cfg.cid = cid
	})
}

// WithReplicationFactor sets replication factor.
func WithReplicationFactor(replicationFactor int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.replicationFactor = replicationFactor
	})
}

// WithRPCAddr sets an address for RPC communication.
func WithRPCAddr(rpcAddr string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.rpcAddr = rpcAddr
	})
}

// WithRaftURL sets an URL for Raft.
func WithRaftURL(raftURL string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.raftURL = raftURL
	})
}

// WithRaftDir sets a directory for the Raft data.
func WithRaftDir(raftDir string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.raftDir = raftDir
	})
}

// WithLogDir sets a directory for logs.
func WithLogDir(logDir string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logDir = logDir
	})
}

// WithPeers sets peer nodes for Raft.
func WithPeers(peers []string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.peers = peers
	})
}

// WithLogger sets a logger.
func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}
