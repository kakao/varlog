package mrmanager

import (
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultInitialMRConnectRetryCount   = -1
	DefaultInitialMRConnectRetryBackoff = 100 * time.Millisecond
	DefaultMRConnTimeout                = 1 * time.Second
	DefaultMRCallTimeout                = 3 * time.Second
)

type config struct {
	cid                         types.ClusterID
	metadataRepositoryAddresses []string
	initialMRConnRetryCount     int
	initialMRConnRetryBackoff   time.Duration
	connTimeout                 time.Duration
	callTimeout                 time.Duration
	logger                      *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		initialMRConnRetryCount:   DefaultInitialMRConnectRetryCount,
		initialMRConnRetryBackoff: DefaultInitialMRConnectRetryBackoff,
		connTimeout:               DefaultMRConnTimeout,
		callTimeout:               DefaultMRCallTimeout,
		logger:                    zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	cfg.logger = cfg.logger.Named("mr manager")
	return cfg, nil
}

func (cfg *config) validate() error {
	if len(cfg.metadataRepositoryAddresses) == 0 {
		return errors.New("no metadata repository address")
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

func WithAddresses(addrs ...string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.metadataRepositoryAddresses = addrs
	})
}

func WithInitialMRConnRetryCount(retry int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.initialMRConnRetryCount = retry
	})
}

func WithInitialMRConnRetryBackoff(backoff time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.initialMRConnRetryBackoff = backoff
	})
}

func WithMRManagerConnTimeout(connTimeout time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.connTimeout = connTimeout
	})
}

func WithMRManagerCallTimeout(callTimeout time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.callTimeout = callTimeout
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}
