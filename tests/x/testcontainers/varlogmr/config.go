package varlogmr

import "github.com/kakao/varlog/pkg/types"

type config struct {
	cid       types.ClusterID
	repfactor int
	rpcPort   int
	raftPort  int
	host      string
}

func newConfig(opts []Option) (config, error) {
	cfg := config{}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg, nil
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
		cfg.cid = cid
	})
}

func WithReplicationFactor(repfactor int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.repfactor = repfactor
	})
}

func WithRPCPort(port int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.rpcPort = port
	})
}

func WithRAFTPort(port int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.raftPort = port
	})
}

func WithHost(host string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.host = host
	})
}
