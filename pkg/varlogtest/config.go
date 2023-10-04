package varlogtest

import (
	"fmt"

	"github.com/kakao/varlog/pkg/types"
)

type config struct {
	clusterID         types.ClusterID
	replicationFactor int
}

func newConfig(opts []Option) (config, error) {
	cfg := config{}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg *config) validate() error {
	if cfg.clusterID.Invalid() {
		return fmt.Errorf("invalid cluster id %d", cfg.clusterID)
	}
	if cfg.replicationFactor < 1 {
		return fmt.Errorf("invalid replication factor %d", cfg.replicationFactor)
	}
	return nil
}

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

func WithClusterID(cid types.ClusterID) Option {
	return newFuncOption(func(cfg *config) {
		cfg.clusterID = cid
	})
}

func WithReplicationFactor(repfactor int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.replicationFactor = repfactor
	})
}
