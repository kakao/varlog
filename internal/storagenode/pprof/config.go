package pprof

import "time"

const (
	DefaultPProfReadHeaderTimeout = 5 * time.Second
	DefaultPProfWriteTimeout      = 11 * time.Second
	DefaultPProfIdleTimeout       = 120 * time.Second
)

type config struct {
	readHeaderTimeout time.Duration
	writeTimeout      time.Duration
	idleTimeout       time.Duration
}

func newConfig(opts []Option) config {
	cfg := config{
		readHeaderTimeout: DefaultPProfReadHeaderTimeout,
		writeTimeout:      DefaultPProfWriteTimeout,
		idleTimeout:       DefaultPProfIdleTimeout,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
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

func WithReadHeaderTimeout(readHeaderTimeout time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.readHeaderTimeout = readHeaderTimeout
	})
}

func WithWriteTimeout(writeTimeout time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.writeTimeout = writeTimeout
	})
}

func WithIdleTimeout(idleTimeout time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.idleTimeout = idleTimeout
	})
}
