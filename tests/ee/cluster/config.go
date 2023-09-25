package cluster

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/pkg/types"
)

const (
	defaultClusterID         = flags.DefaultClusterID
	DefaultReplicationFactor = 3
	DefaultNumMetaRepos      = 3
)

type Config struct {
	cid               types.ClusterID
	replicationFactor int
	numMetaRepos      int
	numStorageNodes   int
	logger            *zap.Logger
}

func NewConfig(t *testing.T, opts ...Option) (Config, error) {
	cfg := Config{
		cid:               defaultClusterID,
		replicationFactor: DefaultReplicationFactor,
		numMetaRepos:      DefaultNumMetaRepos,
		logger:            zaptest.NewLogger(t),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg, nil
}

func (c Config) ClusterID() types.ClusterID {
	return c.cid
}

func (c Config) ReplicationFactor() int {
	return c.replicationFactor
}

func (c Config) NumMetaRepos() int {
	return c.numMetaRepos
}

func (c Config) NumStorageNodes() int {
	return c.numStorageNodes
}

func (c Config) Logger() *zap.Logger {
	return c.logger
}

type Option interface {
	apply(*Config)
}

type funcOption struct {
	f func(*Config)
}

func newFuncOption(f func(*Config)) *funcOption {
	return &funcOption{f: f}
}

func (fo *funcOption) apply(cfg *Config) {
	fo.f(cfg)
}

func WithClusterID(cid types.ClusterID) Option {
	return newFuncOption(func(cfg *Config) {
		cfg.cid = cid
	})
}

func WithReplicationFactor(replicationFactor int) Option {
	return newFuncOption(func(cfg *Config) {
		cfg.replicationFactor = replicationFactor
	})
}

func NumMetadataRepositoryNodes(numMetaReposNodes int) Option {
	return newFuncOption(func(cfg *Config) {
		cfg.numMetaRepos = numMetaReposNodes
	})
}

func NumStorageNodes(numStorageNodes int) Option {
	return newFuncOption(func(cfg *Config) {
		cfg.numStorageNodes = numStorageNodes
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *Config) {
		cfg.logger = logger
	})
}
