package mrconnector

import (
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
)

const (
	defaultClusterID         = flags.DefaultClusterID
	defaultConnTimeout       = 1 * time.Second
	defaultRPCTimeout        = 1 * time.Second
	defaultInitCount         = 10
	defaultInitRetryInterval = 100 * time.Millisecond
	defaultUpdateInterval    = 1 * time.Second
)

type config struct {
	clusterID              types.ClusterID
	connTimeout            time.Duration
	rpcTimeout             time.Duration
	initCount              int
	initRetryInterval      time.Duration
	updateInterval         time.Duration
	seed                   []string
	logger                 *zap.Logger
	defaultGRPCDialOptions []grpc.DialOption
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		clusterID:         defaultClusterID,
		connTimeout:       defaultConnTimeout,
		rpcTimeout:        defaultRPCTimeout,
		initCount:         defaultInitCount,
		initRetryInterval: defaultInitRetryInterval,
		updateInterval:    defaultUpdateInterval,
		logger:            zap.NewNop(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *config) validate() error {
	if c.initCount < 1 {
		return errors.Wrap(verrors.ErrInvalid, "mrconnector: non positive init count")
	}
	if len(c.seed) < 1 {
		return errors.Wrap(verrors.ErrInvalid, "mrconnector: no seed")
	}
	if c.logger == nil {
		return errors.Wrap(verrors.ErrInvalid, "mrconnector: no logger")
	}
	return nil
}

type Option func(*config)

// WithClusterID sets cluster ID of MR cluster.
func WithClusterID(clusterID types.ClusterID) Option {
	return func(opts *config) {
		opts.clusterID = clusterID
	}
}

// WithConnectTimeout sets a timeout to connect to the MR.
func WithConnectTimeout(connTimeout time.Duration) Option {
	return func(opts *config) {
		opts.connTimeout = connTimeout
	}
}

// WithRPCTimeout sets a timeout to call RPC methods to the MR.
func WithRPCTimeout(rpcTimeout time.Duration) Option {
	return func(opts *config) {
		opts.rpcTimeout = rpcTimeout
	}
}

// WithInitCount sets the maximum number of tries to connect to the given seed and fetch addresses
// of MRs.
func WithInitCount(initCount int) Option {
	return func(opts *config) {
		opts.initCount = initCount
	}
}

// WithInitRetryInterval sets the interval between tries to initialize.
func WithInitRetryInterval(interval time.Duration) Option {
	return func(opts *config) {
		opts.initRetryInterval = interval
	}
}

// WithUpdateInterval sets the interval between updates of information of MR cluster.
func WithUpdateInterval(interval time.Duration) Option {
	return func(opts *config) {
		opts.updateInterval = interval
	}
}

// WithSeed sets seed addresses of MRs that can be used to fetch addresses of MRs initially. Seed is
// an immutable list of addresses of MRs.
func WithSeed(seed []string) Option {
	return func(opts *config) {
		opts.seed = seed
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(opts *config) {
		opts.logger = logger
	}
}

func WithDefaultGRPCDialOptions(grpcDialOpts ...grpc.DialOption) Option {
	return func(opts *config) {
		opts.defaultGRPCDialOptions = grpcDialOpts
	}
}
