package varlog

import (
	"math"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/kakao/varlog/pkg/types"
)

const (
	defaultOpenTimeout = 10 * time.Second

	defaultMRConnectorConnTimeout = 1 * time.Second
	defaultMRConnectorCallTimeout = 1 * time.Second

	defaultMetadataRefreshInterval = 1 * time.Minute
	defaultMetadataRefreshTimeout  = 1 * time.Second

	defaultSubscribeTimeout = 10 * time.Millisecond

	defaultDenyTTL            = 10 * time.Minute
	defaultExpireDenyInterval = 1 * time.Second
)

func defaultOptions() options {
	return options{
		openTimeout: defaultOpenTimeout,

		mrConnectorConnTimeout: defaultMRConnectorConnTimeout,
		mrConnectorCallTimeout: defaultMRConnectorCallTimeout,

		metadataRefreshInterval: defaultMetadataRefreshInterval,
		metadataRefreshTimeout:  defaultMetadataRefreshTimeout,

		denyTTL:            defaultDenyTTL,
		expireDenyInterval: defaultExpireDenyInterval,
		logger:             zap.NewNop(),
		grpcDialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				// They set the maximum message size the client can read and
				// write. We use the `math.MaxInt32` for those values, which
				// are the full size possible.
				grpc.MaxCallRecvMsgSize(math.MaxInt32),
				grpc.MaxCallSendMsgSize(math.MaxInt32),
			),
		},
	}
}

type options struct {
	// openTimeout is the timeout for opening a log.
	openTimeout time.Duration

	// mrconnector
	mrConnectorConnTimeout time.Duration
	mrConnectorCallTimeout time.Duration

	// metadata refresher
	// metadataRefreshInterval is the period to refresh metadata.
	metadataRefreshInterval time.Duration
	metadataRefreshTimeout  time.Duration

	// denyTTL is duration until the log stream in denylist is expired and goes back to
	// allowlist.
	denyTTL            time.Duration
	expireDenyInterval time.Duration

	// grpcOptions
	grpcDialOptions []grpc.DialOption

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

func WithMRConnectorConnTimeout(timeout time.Duration) Option {
	return newOption(func(opts *options) {
		opts.mrConnectorConnTimeout = timeout
	})
}

func WithMRConnectorCallTimeout(timeout time.Duration) Option {
	return newOption(func(opts *options) {
		opts.mrConnectorCallTimeout = timeout
	})
}

func WithMetadataRefreshInterval(interval time.Duration) Option {
	return newOption(func(opts *options) {
		opts.metadataRefreshInterval = interval
	})
}

func WithMetadataRefreshTimeout(timeout time.Duration) Option {
	return newOption(func(opts *options) {
		opts.metadataRefreshTimeout = timeout
	})
}

func WithDenyTTL(denyTTL time.Duration) Option {
	return newOption(func(opts *options) {
		opts.denyTTL = denyTTL
	})
}

func WithExpireDenyInterval(interval time.Duration) Option {
	return newOption(func(opts *options) {
		opts.expireDenyInterval = interval
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newOption(func(opts *options) {
		opts.logger = logger
	})
}

// WithGRPCReadBufferSize sets the size of the gRPC read buffer. Internally, it
// calls `google.golang.org/grpc.WithReadBufferSize`.
func WithGRPCReadBufferSize(bytes int) Option {
	return newOption(func(opts *options) {
		opts.grpcDialOptions = append(opts.grpcDialOptions,
			grpc.WithReadBufferSize(bytes),
		)
	})
}

// WithGRPCWriteBufferSize sets the size of the gRPC write buffer. Internally,
// it calls `google.golang.org/grpc.WithWriteBufferSize`.
func WithGRPCWriteBufferSize(bytes int) Option {
	return newOption(func(opts *options) {
		opts.grpcDialOptions = append(opts.grpcDialOptions,
			grpc.WithWriteBufferSize(bytes),
		)
	})
}

// WithGRPCInitialConnWindowSize sets the initial window size on a connection.
// Internally, it calls `google.golang.org/grpc.WithInitialConnWindowSize`.
func WithGRPCInitialConnWindowSize(bytes int32) Option {
	return newOption(func(opts *options) {
		opts.grpcDialOptions = append(opts.grpcDialOptions,
			grpc.WithInitialConnWindowSize(bytes),
		)
	})
}

// WithGRPCInitialWindowSize sets the initial window size on a stream.
// Internally, it calls `google.golang.org/grpc.WithInitialWindowSize`.
func WithGRPCInitialWindowSize(bytes int32) Option {
	return newOption(func(opts *options) {
		opts.grpcDialOptions = append(opts.grpcDialOptions,
			grpc.WithInitialWindowSize(bytes),
		)
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
	retryCount        int
	selectLogStream   bool
	allowedLogStreams map[types.LogStreamID]struct{}
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

func WithAllowedLogStreams(logStreams map[types.LogStreamID]struct{}) AppendOption {
	return newAppendOption(func(opts *appendOptions) {
		opts.allowedLogStreams = logStreams
	})
}

func defaultSubscribeOptions() subscribeOptions {
	return subscribeOptions{
		timeout: defaultSubscribeTimeout,
	}
}

type subscribeOptions struct {
	timeout time.Duration
}

type SubscribeOption interface {
	apply(*subscribeOptions)
}

type subscribeOption struct {
	f func(*subscribeOptions)
}

func (opt *subscribeOption) apply(opts *subscribeOptions) {
	opt.f(opts)
}

func newSubscribeOption(f func(*subscribeOptions)) *subscribeOption {
	return &subscribeOption{f: f}
}

func WithSubscribeTimeout(timeout time.Duration) SubscribeOption {
	return newSubscribeOption(func(opts *subscribeOptions) {
		opts.timeout = timeout
	})
}
