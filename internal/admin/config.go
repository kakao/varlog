package admin

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/internal/admin/snmanager"
	"github.com/kakao/varlog/internal/admin/snwatcher"
	"github.com/kakao/varlog/internal/admin/stats"
	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/pkg/types"
)

const (
	defaultClusterID          = flags.DefaultClusterID
	DefaultListenAddress      = "127.0.0.1:9090"
	DefaultReplicationFactor  = flags.DefaultReplicationFactor
	DefaultLogStreamGCTimeout = 24 * time.Hour
)

type config struct {
	cid                      types.ClusterID
	listenAddress            string
	replicationFactor        int
	logStreamGCTimeout       time.Duration
	disableAutoLogStreamSync bool
	enableAutoUnseal         bool
	mrmgr                    mrmanager.MetadataRepositoryManager
	snmgr                    snmanager.StorageNodeManager
	snSelector               ReplicaSelector
	statRepository           stats.Repository
	snwatcherOpts            []snwatcher.Option
	defaultGRPCServerOptions []grpc.ServerOption
	logger                   *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		cid:                defaultClusterID,
		listenAddress:      DefaultListenAddress,
		replicationFactor:  DefaultReplicationFactor,
		logStreamGCTimeout: DefaultLogStreamGCTimeout,
		logger:             zap.NewNop(),
	}

	for _, opt := range opts {
		opt.apply(&cfg)
	}

	if err := cfg.validate(); err != nil {
		return cfg, err
	}

	if err := cfg.ensureDefault(); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func (cfg config) validate() error {
	if len(cfg.listenAddress) == 0 {
		return errors.New("no listen address")
	}
	if cfg.replicationFactor < 1 {
		return errors.New("non-positive replication factor")
	}
	if cfg.mrmgr == nil {
		return errors.New("mr manager is nil")
	}
	if cfg.snmgr == nil {
		return errors.New("sn manager is nil")
	}
	if cfg.logger == nil {
		return errors.New("logger is nil")
	}
	return nil
}

func (cfg *config) ensureDefault() error {
	if cfg.snSelector == nil {
		rs, err := NewReplicaSelector(ReplicaSelectorNameLFU, cfg.mrmgr.ClusterMetadataView(), int(cfg.replicationFactor))
		if err != nil {
			return err
		}
		cfg.snSelector = rs
	}

	if cfg.statRepository == nil {
		cfg.statRepository = stats.NewRepository(context.TODO(), cfg.mrmgr.ClusterMetadataView())
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

func WithClusterID(cid types.ClusterID) Option {
	return newFuncOption(func(cfg *config) {
		cfg.cid = cid
	})
}

func WithListenAddress(listen string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.listenAddress = listen
	})
}

func WithReplicationFactor(replicationFactor int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.replicationFactor = replicationFactor
	})
}

// WithLogStreamGCTimeout sets expiration duration for garbage log streams.
// To turn off log stream GC, a very large value can be set.
func WithLogStreamGCTimeout(logStreamGCTimeout time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logStreamGCTimeout = logStreamGCTimeout
	})
}

// WithoutAutoLogStreamSync disables automatic sync job between replicas in the log stream.
func WithoutAutoLogStreamSync() Option {
	return newFuncOption(func(cfg *config) {
		cfg.disableAutoLogStreamSync = true
	})
}

func WithAutoUnseal() Option {
	return newFuncOption(func(cfg *config) {
		cfg.enableAutoUnseal = true
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}

func WithMetadataRepositoryManager(mrMgr mrmanager.MetadataRepositoryManager) Option {
	return newFuncOption(func(cfg *config) {
		cfg.mrmgr = mrMgr
	})
}

func WithStorageNodeManager(snMgr snmanager.StorageNodeManager) Option {
	return newFuncOption(func(cfg *config) {
		cfg.snmgr = snMgr
	})
}

func WithReplicaSelector(replicaSelector ReplicaSelector) Option {
	return newFuncOption(func(cfg *config) {
		cfg.snSelector = replicaSelector
	})
}

func WithStatisticsRepository(statsRepos stats.Repository) Option {
	return newFuncOption(func(cfg *config) {
		cfg.statRepository = statsRepos
	})
}

func WithStorageNodeWatcherOptions(opts ...snwatcher.Option) Option {
	return newFuncOption(func(cfg *config) {
		cfg.snwatcherOpts = opts
	})
}

func WithDefaultGRPCServerOptions(grpcServerOptions ...grpc.ServerOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.defaultGRPCServerOptions = grpcServerOptions
	})
}
