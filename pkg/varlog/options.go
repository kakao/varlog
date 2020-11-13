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

const (
	defaultRetryCount = 3
)

func defaultAppendOptions() appendOptions {
	return appendOptions{
		retryCount:      defaultRetryCount,
		selectLogStream: true,
	}
}

type appendOptions struct {
	retryCount      int
	selectLogStream bool
}

type AppendOption interface {
	apply(*appendOptions)
}

type appendOption struct {
	f func(*appendOptions)
}

func (opt *appendOption) apply(opts *appendOptions) {
	opt.f(opts)
}

func newAppendOption(f func(*appendOptions)) *appendOption {
	return &appendOption{f: f}
}

func WithRetryCount(retryCount int) AppendOption {
	return newAppendOption(func(opts *appendOptions) {
		opts.retryCount = retryCount
	})
}

func withoutSelectLogStream() AppendOption {
	return newAppendOption(func(opts *appendOptions) {
		opts.selectLogStream = false
	})
}
