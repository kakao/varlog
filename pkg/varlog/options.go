package varlog

import (
	"time"

	"go.uber.org/zap"
)

const (
	defaultOpenTimeout             = 10 * time.Second
	defaultMetadataRefreshInterval = 1 * time.Minute
	defaultDenyTTL                 = 10 * time.Minute
)

func defaultOptions() options {
	return options{
		openTimeout:             defaultOpenTimeout,
		metadataRefreshInterval: defaultMetadataRefreshInterval,
		denyTTL:                 defaultDenyTTL,
		logger:                  zap.NewNop(),
	}
}

type options struct {
	// openTimeout is the timeout for opening a log.
	openTimeout time.Duration

	// metadataRefreshInterval is the period to refresh metadata.
	metadataRefreshInterval time.Duration

	// denyTTL is duration until the log stream in denylist is expired and goes back to
	// allowlist.
	denyTTL time.Duration

	logger *zap.Logger
}

type Option interface {
	apply(*options)
}

type option struct {
	f func(*options)
}

func newOption(f func(*options)) *option {
	return &option{f: f}
}

func (opt *option) apply(opts *options) {
	opt.f(opts)
}

func WithOpenTimeout(timeout time.Duration) Option {
	return newOption(func(opts *options) {
		opts.openTimeout = timeout
	})
}

func WithMetadataRefreshInterval(interval time.Duration) Option {
	return newOption(func(opts *options) {
		opts.metadataRefreshInterval = interval
	})
}

func WithDenyTTL(denyTTL time.Duration) Option {
	return newOption(func(opts *options) {
		opts.denyTTL = denyTTL
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newOption(func(opts *options) {
		opts.logger = logger
	})
}

type AppendOptions struct {
	Retry           int
	selectLogStream bool
}

type ReadOptions struct{}

type SubscribeOptions struct{}

type TrimOptions struct{}
