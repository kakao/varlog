package snmanager

import (
	"errors"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/admin/mrmanager"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type config struct {
	cid    types.ClusterID
	cmview mrmanager.ClusterMetadataView
	logger *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	cfg.logger = cfg.logger.Named("sn manager")
	return cfg, nil
}

func (cfg *config) validate() error {
	if cfg.cmview == nil {
		return errors.New("nil cluster metadata view")
	}
	if cfg.logger == nil {
		return errors.New("nil logger")
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
		cfg.cid = cid
	})
}

func WithClusterMetadataView(cmview mrmanager.ClusterMetadataView) Option {
	return newFuncOption(func(cfg *config) {
		cfg.cmview = cmview
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}
