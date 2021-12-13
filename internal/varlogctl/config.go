package varlogctl

import (
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

type config struct {
	admin       varlog.Admin
	pretty      bool
	executeFunc ExecuteFunc
	logger      *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg, nil
}

type Option func(*config)

func WithAdmin(admin varlog.Admin) Option {
	return func(cfg *config) {
		cfg.admin = admin
	}
}

func WithPrettyPrint() Option {
	return func(cfg *config) {
		cfg.pretty = true
	}
}

func WithExecuteFunc(executeFunc ExecuteFunc) Option {
	return func(cfg *config) {
		cfg.executeFunc = executeFunc
	}
}

func WithLogger(loger *zap.Logger) Option {
	return func(cfg *config) {
		cfg.logger = loger
	}
}
