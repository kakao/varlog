package mrconnector

import (
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

const (
	defaultConnectionTimeout          = 5 * time.Second
	defaultRPCAddrsFetchRetryInterval = 100 * time.Millisecond
	defaultClusterInfoFetchInterval   = 10 * time.Second
)

type options struct {
	clusterID                  types.ClusterID
	connectionTimeout          time.Duration
	rpcAddrsFetchRetryInterval time.Duration
	clusterInfoFetchInterval   time.Duration
	logger                     *zap.Logger
}

func defaultOptions() options {
	return options{
		clusterID:                  types.ClusterID(1),
		connectionTimeout:          defaultConnectionTimeout,
		rpcAddrsFetchRetryInterval: defaultRPCAddrsFetchRetryInterval,
		clusterInfoFetchInterval:   defaultClusterInfoFetchInterval,
		logger:                     zap.NewNop(),
	}
}

type Option func(*options)

func WithClusterID(clusterID types.ClusterID) Option {
	return func(opts *options) {
		opts.clusterID = clusterID
	}
}

func WithConnectionTimeout(timeout time.Duration) Option {
	return func(opts *options) {
		opts.connectionTimeout = timeout
	}
}

func WithRPCAddrsFetchRetryInterval(interval time.Duration) Option {
	return func(opts *options) {
		opts.rpcAddrsFetchRetryInterval = interval
	}
}

func WithClusterInfoFetchInterval(interval time.Duration) Option {
	return func(opts *options) {
		opts.clusterInfoFetchInterval = interval
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(opts *options) {
		opts.logger = logger
	}
}
