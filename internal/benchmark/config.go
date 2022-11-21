package benchmark

import (
	"errors"
	"time"

	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultClusterID      = types.ClusterID(1)
	DefaultMessageSize    = 0
	DefaultBatchSize      = 1
	DefaultConcurrency    = 0
	DefaultDuration       = 1 * time.Minute
	DefaultReportInterval = 3 * time.Second
)

type config struct {
	cid            types.ClusterID
	targets        []Target
	mraddrs        []string
	duration       time.Duration
	reportInterval time.Duration
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		cid:            DefaultClusterID,
		duration:       DefaultDuration,
		reportInterval: DefaultReportInterval,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg *config) validate() error {
	if len(cfg.mraddrs) == 0 {
		return errors.New("no metadata repository address")
	}
	if len(cfg.targets) == 0 {
		return errors.New("no load target")
	}
	for _, target := range cfg.targets {
		if err := target.Valid(); err != nil {
			return err
		}
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

func WithTargets(targets ...Target) Option {
	return newFuncOption(func(cfg *config) {
		cfg.targets = targets
	})
}

func WithMetadataRepository(addrs []string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.mraddrs = addrs
	})
}

func WithDuration(duration time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.duration = duration
	})
}

func WithReportInterval(reportInterval time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.reportInterval = reportInterval
	})
}
