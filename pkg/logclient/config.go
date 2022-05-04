package logclient

import (
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type managerConfig struct {
	defaultGRPCDialOptions []grpc.DialOption
	logger                 *zap.Logger
}

func newManagerConfig(opts []ManagerOption) (managerConfig, error) {
	cfg := managerConfig{
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt.applyManager(&cfg)
	}
	cfg.logger = cfg.logger.Named("logclient manager")
	return cfg, cfg.validate()
}

func (cfg *managerConfig) validate() error {
	if cfg.logger == nil {
		return errors.New("logger is nil")
	}
	return nil
}

type ManagerOption interface {
	applyManager(*managerConfig)
}

type funcManagerOption struct {
	f func(*managerConfig)
}

func newFuncManagerOption(f func(*managerConfig)) *funcManagerOption {
	return &funcManagerOption{f: f}
}

func (fmo *funcManagerOption) applyManager(cfg *managerConfig) {
	fmo.f(cfg)
}

func WithDefaultGRPCDialOptions(defaultGRPCDialOptions ...grpc.DialOption) ManagerOption {
	return newFuncManagerOption(func(cfg *managerConfig) {
		cfg.defaultGRPCDialOptions = defaultGRPCDialOptions
	})
}

func WithLogger(logger *zap.Logger) ManagerOption {
	return newFuncManagerOption(func(cfg *managerConfig) {
		cfg.logger = logger
	})
}