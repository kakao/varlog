package varlog

import (
	"context"
	"time"

	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
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

const (
	defaultPipelineSize = 2
	minPipelineSize     = storagenode.MinAppendPipelineSize
	maxPipelineSize     = storagenode.MaxAppendPipelineSize
)

type logStreamAppenderConfig struct {
	defaultBatchCallback BatchCallback
	tpid                 types.TopicID
	lsid                 types.LogStreamID
	pipelineSize         int
	callTimeout          time.Duration
}

func newLogStreamAppenderConfig(opts []LogStreamAppenderOption) logStreamAppenderConfig {
	cfg := logStreamAppenderConfig{
		pipelineSize:         defaultPipelineSize,
		defaultBatchCallback: func([]varlogpb.LogEntryMeta, error) {},
	}
	for _, opt := range opts {
		opt.applyLogStreamAppender(&cfg)
	}
	cfg.ensureDefault()
	return cfg
}

func (cfg *logStreamAppenderConfig) ensureDefault() {
	if cfg.pipelineSize < minPipelineSize {
		cfg.pipelineSize = minPipelineSize
	}
	if cfg.pipelineSize > maxPipelineSize {
		cfg.pipelineSize = maxPipelineSize
	}
}

type funcLogStreamAppenderOption struct {
	f func(*logStreamAppenderConfig)
}

func newFuncLogStreamAppenderOption(f func(config *logStreamAppenderConfig)) *funcLogStreamAppenderOption {
	return &funcLogStreamAppenderOption{f: f}
}

func (fo *funcLogStreamAppenderOption) applyLogStreamAppender(cfg *logStreamAppenderConfig) {
	fo.f(cfg)
}

// LogStreamAppenderOption configures a LogStreamAppender.
type LogStreamAppenderOption interface {
	applyLogStreamAppender(config *logStreamAppenderConfig)
}

// WithPipelineSize sets request pipeline size. The default pipeline size is
// two. Any value below one will be set to one, and any above eight will be
// limited to eight.
func WithPipelineSize(pipelineSize int) LogStreamAppenderOption {
	return newFuncLogStreamAppenderOption(func(cfg *logStreamAppenderConfig) {
		cfg.pipelineSize = pipelineSize
	})
}

// WithDefaultBatchCallback sets the default callback function. The default callback
// function can be overridden by the argument callback of the AppendBatch
// method.
func WithDefaultBatchCallback(defaultBatchCallback BatchCallback) LogStreamAppenderOption {
	return newFuncLogStreamAppenderOption(func(cfg *logStreamAppenderConfig) {
		cfg.defaultBatchCallback = defaultBatchCallback
	})
}

// WithCallTimeout configures a timeout for each AppendBatch call. If the
// timeout has elapsed, the AppendBatch and callback functions may result in an
// ErrCallTimeout error.
//
// ErrCallTimeout may be returned in the following scenarios:
// - Waiting for the pipeline too long since it is full.
// - Sending RPC requests to the varlog is blocked for too long.
// - Receiving RPC response from the varlog is blocked too long.
// - User codes for callback take time too long.
func WithCallTimeout(callTimeout time.Duration) LogStreamAppenderOption {
	return newFuncLogStreamAppenderOption(func(cfg *logStreamAppenderConfig) {
		cfg.callTimeout = callTimeout
	})
}
