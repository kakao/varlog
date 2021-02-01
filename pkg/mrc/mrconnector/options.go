package mrconnector

import (
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

const (
	defaultConnectionTimeout                 = 5 * time.Second
	defaultRPCAddrsInitialFetchRetryInterval = 100 * time.Millisecond
	defaultRPCAddrsFetchInterval             = 10 * time.Second
	defaultRPCAddrsFetchTimeout              = 100 * time.Millisecond
)

type options struct {
	clusterID                         types.ClusterID
	connectionTimeout                 time.Duration
	rpcAddrsInitialFetchRetryInterval time.Duration
	rpcAddrsFetchInterval             time.Duration
	rpcAddrsFetchTimeout              time.Duration
	logger                            *zap.Logger
}

func defaultOptions() options {
	return options{
		clusterID:                         types.ClusterID(1),
		connectionTimeout:                 defaultConnectionTimeout,
		rpcAddrsInitialFetchRetryInterval: defaultRPCAddrsInitialFetchRetryInterval,
		rpcAddrsFetchInterval:             defaultRPCAddrsFetchInterval,
		rpcAddrsFetchTimeout:              defaultRPCAddrsFetchTimeout,
		logger:                            zap.NewNop(),
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

func WithRPCAddrsInitialFetchRetryInterval(interval time.Duration) Option {
	return func(opts *options) {
		opts.rpcAddrsInitialFetchRetryInterval = interval
	}
}

func WithRPCAddrsFetchInterval(interval time.Duration) Option {
	return func(opts *options) {
		opts.rpcAddrsFetchInterval = interval
	}
}

func WithRPCAddrsFetchTimeout(timeout time.Duration) Option {
	return func(opts *options) {
		opts.rpcAddrsFetchTimeout = timeout
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(opts *options) {
		opts.logger = logger
	}
}
