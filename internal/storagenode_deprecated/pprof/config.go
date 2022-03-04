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

type readHeaderTimeoutOption time.Duration

func (o readHeaderTimeoutOption) apply(c *config) {
	c.readHeaderTimeout = time.Duration(o)
}

func WithReadHeaderTimeout(timeout time.Duration) Option {
	return readHeaderTimeoutOption(timeout)
}

type writeTimeoutOption time.Duration

func (o writeTimeoutOption) apply(c *config) {
	c.writeTimeout = time.Duration(o)
}

func WithWriteTimeout(timeout time.Duration) Option {
	return writeTimeoutOption(timeout)
}

type idleTimeoutOption time.Duration

func (o idleTimeoutOption) apply(c *config) {
	c.idleTimeout = time.Duration(o)
}

func WithIdleTimeout(timeout time.Duration) Option {
	return idleTimeoutOption(timeout)
}
