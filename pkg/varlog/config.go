package varlog

import (
	"context"
	"time"
)

type adminConfig struct {
	adminCallOptions []AdminCallOption
}

func newAdminConfig(opts []AdminOption) adminConfig {
	cfg := adminConfig{}
	for _, opt := range opts {
		opt.applyAdmin(&cfg)
	}
	return cfg
}

// AdminOption configures the admin client.
type AdminOption interface {
	applyAdmin(*adminConfig)
}

type funcAdminOption struct {
	f func(*adminConfig)
}

func newFuncAdminOption(f func(*adminConfig)) *funcAdminOption {
	return &funcAdminOption{f: f}
}

func (fao *funcAdminOption) applyAdmin(cfg *adminConfig) {
	fao.f(cfg)
}

// WithDefaultAdminCallOptions sets the default AdminCallOptions for all RPC calls over the connection.
func WithDefaultAdminCallOptions(opts ...AdminCallOption) AdminOption {
	return newFuncAdminOption(func(cfg *adminConfig) {
		cfg.adminCallOptions = opts
	})
}

type adminCallConfig struct {
	timeout struct {
		time.Duration
		set bool
	}
}

func newAdminCallConfig(defaultOpts []AdminCallOption, opts []AdminCallOption) adminCallConfig {
	cfg := adminCallConfig{}
	combinedOpts := append(defaultOpts, opts...)
	for _, opt := range combinedOpts {
		opt.applyAdminCall(&cfg)
	}
	return cfg
}

func (cfg *adminCallConfig) withTimeoutContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if !cfg.timeout.set {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, cfg.timeout.Duration)
}

// AdminCallOption configures the RPC calls to the admin.
type AdminCallOption interface {
	applyAdminCall(*adminCallConfig)
}

type funcAdminCallOption struct {
	f func(*adminCallConfig)
}

func newFuncAdminCallOption(f func(*adminCallConfig)) *funcAdminCallOption {
	return &funcAdminCallOption{f: f}
}

func (faco *funcAdminCallOption) applyAdminCall(cfg *adminCallConfig) {
	faco.f(cfg)
}

// WithTimeout sets the timeout of the call.
// It sets context timeout based on the parent context given to each method.
// The default timeout configured when a client is created is overridden by the
// timeout option given to each method.
func WithTimeout(timeout time.Duration) AdminCallOption {
	return newFuncAdminCallOption(func(cfg *adminCallConfig) {
		cfg.timeout.Duration = timeout
		cfg.timeout.set = true
	})
}
