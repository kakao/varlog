package mrconnector

import (
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

const (
	defaultConnectionTimeout        = 5 * time.Second
	defaultClusterInfoFetchInterval = 100 * time.Millisecond
)

type options struct {
	clusterID                  types.ClusterID
	connectionTimeout          time.Duration
	rpcAddrsFetchRetryInterval time.Duration
	logger                     *zap.Logger
}

var defaultOptions = options{
	clusterID:                  types.ClusterID(1),
	connectionTimeout:          defaultConnectionTimeout,
	rpcAddrsFetchRetryInterval: defaultClusterInfoFetchInterval,
	logger:                     zap.NewNop(),
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

func WithLogger(logger *zap.Logger) Option {
	return func(opts *options) {
		opts.logger = logger
	}
}
