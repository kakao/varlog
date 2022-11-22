package benchmark

import (
	"errors"
	"fmt"
	"time"

	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultClusterID      = types.ClusterID(1)
	DefaultBatchSize      = 1
	DefaultConcurrency    = 1
	DefaultDuration       = 1 * time.Minute
	DefaultReportInterval = 5 * time.Second
)

type config struct {
	cid            types.ClusterID
	tpid           types.TopicID
	lsid           types.LogStreamID
	mraddrs        []string
	msgSize        int
	batchSize      int
	concurrency    int
	duration       time.Duration
	reportInterval time.Duration
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		cid:            DefaultClusterID,
		batchSize:      DefaultBatchSize,
		concurrency:    DefaultConcurrency,
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
	if cfg.tpid.Invalid() {
		return fmt.Errorf("invalid topic %v", cfg.tpid)
	}
	if len(cfg.mraddrs) == 0 {
		return errors.New("no metadata repository address")
	}
	if cfg.batchSize < 1 {
		return fmt.Errorf("non-positive batch size %d", cfg.batchSize)
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

func WithTopicID(tpid types.TopicID) Option {
	return newFuncOption(func(cfg *config) {
		cfg.tpid = tpid
	})
}

func WithLogStreamID(lsid types.LogStreamID) Option {
	return newFuncOption(func(cfg *config) {
		cfg.lsid = lsid
	})
}

func WithMetadataRepository(addrs []string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.mraddrs = addrs
	})
}

func WithMessageSize(msgSize int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.msgSize = msgSize
	})
}

func WithBatchSize(batchSize int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.batchSize = batchSize
	})
}
func WithConcurrency(concurrency int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.concurrency = concurrency
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
