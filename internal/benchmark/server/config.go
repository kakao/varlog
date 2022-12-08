package server

import "errors"

const (
	databaseDriver = "postgres"

	DefaultAddress      = ":0"
	DefaultDatabaseHost = "0.0.0.0"
	DefaultDatabasePort = 5432
	DefaultDatabaseUser = "varlog_benchmark"
	DefaultDatabaseName = "varlog_benchmark"
)

type config struct {
	addr             string
	databaseHost     string
	databasePort     int
	databaseUser     string
	databasePassword string
	databaseName     string
}

func newConfig(opts []Option) (config, error) {
	cfg := config{}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg *config) validate() error {
	if len(cfg.addr) == 0 {
		return errors.New("no bind address")
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

func WithAddress(addr string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.addr = addr
	})
}

func WithDatabaseHost(databaseHost string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.databaseHost = databaseHost
	})
}

func WithDatabasePort(databasePort int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.databasePort = databasePort
	})
}

func WithDatabaseUser(databaseUser string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.databaseUser = databaseUser
	})
}

func WithDatabasePassword(databasePassword string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.databasePassword = databasePassword
	})
}

func WithDatabaseName(databaseName string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.databaseName = databaseName
	})
}
