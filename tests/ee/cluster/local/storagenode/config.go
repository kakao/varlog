package storagenode

import (
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

type config struct {
	executable string
	name       string
	cid        types.ClusterID
	snid       types.StorageNodeID
	addr       string
	volumes    []string
	logger     *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	cfg.logger = cfg.logger.With(zap.Int32("snid", int32(cfg.snid)))
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

func WithExecutable(executable string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.executable = executable
	})
}

func WithNodeName(name string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.name = name
	})
}

func WithClusterID(cid types.ClusterID) Option {
	return newFuncOption(func(cfg *config) {
		cfg.cid = cid
	})
}

func WithStorageNodeID(snid types.StorageNodeID) Option {
	return newFuncOption(func(cfg *config) {
		cfg.snid = snid
	})
}

func WithAddress(addr string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.addr = addr
	})
}

func WithVolumes(volumes ...string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.volumes = volumes
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}
