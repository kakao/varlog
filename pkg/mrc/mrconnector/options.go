package mrconnector

import (
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

const (
	defaultClusterID                         = types.ClusterID(1)
	defaultConnTimeout                       = 1 * time.Second
	defaultRPCTimeout                        = 1 * time.Second
	defaultInitialRPCAddrsFetchRetryCount    = 3
	defaultInitialRPCAddrsFetchRetryInterval = 100 * time.Millisecond
	defaultRPCAddrsFetchInterval             = 10 * time.Second
)

type options struct {
	clusterID                         types.ClusterID
	connTimeout                       time.Duration
	rpcTimeout                        time.Duration
	initialRPCAddrsFetchRetryCount    int
	initialRPCAddrsFetchRetryInterval time.Duration
	rpcAddrsFetchInterval             time.Duration
	logger                            *zap.Logger
}

func defaultOptions() options {
	return options{
		clusterID:                         defaultClusterID,
		connTimeout:                       defaultConnTimeout,
		rpcTimeout:                        defaultRPCTimeout,
		initialRPCAddrsFetchRetryInterval: defaultInitialRPCAddrsFetchRetryInterval,
		rpcAddrsFetchInterval:             defaultRPCAddrsFetchInterval,
		logger:                            zap.NewNop(),
	}
}

type Option func(*options)

func WithClusterID(clusterID types.ClusterID) Option {
	return func(opts *options) {
		opts.clusterID = clusterID
	}
}

func WithConnectTimeout(connTimeout time.Duration) Option {
	return func(opts *options) {
		opts.connTimeout = connTimeout
	}
}

func WithRPCTimeout(rpcTimeout time.Duration) Option {
	return func(opts *options) {
		opts.rpcTimeout = rpcTimeout
	}
}

func WithRPCAddrsInitialFetchRetryInterval(interval time.Duration) Option {
	return func(opts *options) {
		opts.initialRPCAddrsFetchRetryInterval = interval
	}
}

func WithRPCAddrsFetchInterval(interval time.Duration) Option {
	return func(opts *options) {
		opts.rpcAddrsFetchInterval = interval
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(opts *options) {
		opts.logger = logger
	}
}
