package storage

import (
	"errors"
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultMetricsLogInterval = time.Duration(0)

	valueStoreDirName  = "_data"
	commitStoreDirName = "_commit"
)

type config struct {
	path               string
	valueStoreOptions  []StoreOption
	commitStoreOptions []StoreOption
	metricsLogInterval time.Duration
	logger             *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		metricsLogInterval: DefaultMetricsLogInterval,
		logger:             zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg config) validate() error {
	if len(cfg.path) == 0 {
		return errors.New("storage: no path")
	}
	if cfg.logger == nil {
		return errors.New("storage: no logger")
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

func WithPath(path string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.path = path
	})
}

// WithValueStoreOptions adds options for the value store. Options set earlier
// are overridden by options set later.
func WithValueStoreOptions(valueStoreOpts ...StoreOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.valueStoreOptions = slices.Concat(cfg.valueStoreOptions, valueStoreOpts)
	})
}

// WithCommitStoreOptions adds options for the commit store. Options set
// earlier are overridden by options set later.
func WithCommitStoreOptions(commitStoreOpts ...StoreOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.commitStoreOptions = slices.Concat(cfg.commitStoreOptions, commitStoreOpts)
	})
}

func WithMetricsLogInterval(metricsLogInterval time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.metricsLogInterval = metricsLogInterval
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}

type readConfig struct {
	llsn types.LLSN
	glsn types.GLSN
}

func newReadConfig(opts []ReadOption) readConfig {
	cfg := readConfig{}
	for _, opt := range opts {
		opt.applyRead(&cfg)
	}
	return cfg
}

type ReadOption interface {
	applyRead(*readConfig)
}

type funcReadOption struct {
	f func(*readConfig)
}

func newFuncReadOption(f func(*readConfig)) *funcReadOption {
	return &funcReadOption{f: f}
}

func (fro *funcReadOption) applyRead(cfg *readConfig) {
	fro.f(cfg)
}

func AtGLSN(glsn types.GLSN) ReadOption {
	return newFuncReadOption(func(cfg *readConfig) {
		cfg.llsn = types.InvalidLLSN
		cfg.glsn = glsn
	})
}

func AtLLSN(llsn types.LLSN) ReadOption {
	return newFuncReadOption(func(cfg *readConfig) {
		cfg.llsn = llsn
		cfg.glsn = types.InvalidGLSN
	})
}
