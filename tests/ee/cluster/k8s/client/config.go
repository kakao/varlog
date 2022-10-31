package client

import (
	"errors"

	"github.com/kakao/varlog/tests/ee/cluster/k8s/vault"
)

type config struct {
	cluster   string
	context   string
	masterURL string
	user      string
	token     string
}

func newConfig(opts []Option) (config, error) {
	var cfg config
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg *config) validate() error {
	if len(cfg.cluster) == 0 {
		return errors.New("k8s: no cluster ")
	}
	if len(cfg.context) == 0 {
		return errors.New("k8s: no context")
	}
	if len(cfg.masterURL) == 0 {
		return errors.New("k8s: no master URL")
	}
	if len(cfg.user) == 0 {
		return errors.New("k8s: no user")
	}
	if len(cfg.token) == 0 {
		return errors.New("k8s: no token")
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

func WithCluster(cluster string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.cluster = cluster
	})
}

func WithContext(context string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.context = context
	})
}

func WithMasterURL(masterURL string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.masterURL = masterURL
	})
}

func WithUser(user string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.user = user
	})
}

func WithToken(token string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.token = token
	})
}

func WithClusterConnectionInfo(connInfo vault.ClusterConnectionInfo) Option {
	return newFuncOption(func(cfg *config) {
		cfg.cluster = connInfo.Cluster
		cfg.context = connInfo.Context
		cfg.masterURL = connInfo.MasterURL
		cfg.user = connInfo.User
		cfg.token = connInfo.Token
	})
}
