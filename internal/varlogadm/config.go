package varlogadm

import (
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/varlogadm/mrmanager"
	"github.com/kakao/varlog/internal/varlogadm/snmanager"
	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultListenAddress     = "127.0.0.1:9090"
	DefaultWatcherRPCTimeout = 3 * time.Second

	DefaultClusterID         = types.ClusterID(1)
	DefaultReplicationFactor = 1

	DefaultTick             = 100 * time.Millisecond
	DefaultReportInterval   = 10
	DefaultHeartbeatTimeout = 10
	DefaultGCTimeout        = 24 * time.Hour
)

type config struct {
	clusterID         types.ClusterID
	listenAddress     string
	replicationFactor uint
	logger            *zap.Logger

	mrMgr mrmanager.MetadataRepositoryManager
	snMgr snmanager.StorageNodeManager

	watcherOptions []WatcherOption
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		clusterID:         DefaultClusterID,
		listenAddress:     DefaultListenAddress,
		replicationFactor: DefaultReplicationFactor,
		logger:            zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg, cfg.validate()
}

func (cfg config) validate() error {
	if len(cfg.listenAddress) == 0 {
		return errors.New("no listen address")
	}
	if cfg.replicationFactor < 1 {
		return errors.New("non-positive replication factor")
	}
	if cfg.mrMgr == nil {
		return errors.New("mr manager is nil")
	}
	if cfg.snMgr == nil {
		return errors.New("sn manager is nil")
	}
	if cfg.logger == nil {
		return errors.New("logger is nil")
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
		cfg.clusterID = cid
	})
}

func WithListenAddress(listen string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.listenAddress = listen
	})
}

func WithReplicationFactor(replicationFactor uint) Option {
	return newFuncOption(func(cfg *config) {
		cfg.replicationFactor = replicationFactor
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}

func WithMetadataRepositoryManager(mrMgr mrmanager.MetadataRepositoryManager) Option {
	return newFuncOption(func(cfg *config) {
		cfg.mrMgr = mrMgr
	})
}

func WithStorageNodeManager(snMgr snmanager.StorageNodeManager) Option {
	return newFuncOption(func(cfg *config) {
		cfg.snMgr = snMgr
	})
}

func WithWatcherOptions(opts ...WatcherOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.watcherOptions = opts
	})
}

type watcherConfig struct {
	tick             time.Duration
	reportInterval   int
	heartbeatTimeout int
	gcTimeout        time.Duration
	// timeout for heartbeat and report
	rpcTimeout time.Duration
}

func newWatcherConfig(opts []WatcherOption) (watcherConfig, error) {
	cfg := watcherConfig{
		tick:             DefaultTick,
		reportInterval:   DefaultReportInterval,
		heartbeatTimeout: DefaultHeartbeatTimeout,
		gcTimeout:        DefaultGCTimeout,
		rpcTimeout:       DefaultWatcherRPCTimeout,
	}
	for _, opt := range opts {
		opt.applyWatcher(&cfg)
	}
	return cfg, cfg.validate()
}

func (cfg *watcherConfig) validate() error {
	return nil
}

type WatcherOption interface {
	applyWatcher(*watcherConfig)
}

type funcWatcherOption struct {
	f func(*watcherConfig)
}

func newFuncWatcherOption(f func(*watcherConfig)) *funcWatcherOption {
	return &funcWatcherOption{f: f}
}

func (fmo *funcWatcherOption) applyWatcher(cfg *watcherConfig) {
	fmo.f(cfg)
}

func WithWatcherTick(tick time.Duration) WatcherOption {
	return newFuncWatcherOption(func(cfg *watcherConfig) {
		cfg.tick = tick
	})
}

func WithWatcherReportInterval(reportInterval int) WatcherOption {
	return newFuncWatcherOption(func(cfg *watcherConfig) {
		cfg.reportInterval = reportInterval
	})
}

func WithWatcherHeartbeatTimeout(heartbeatTimeout int) WatcherOption {
	return newFuncWatcherOption(func(cfg *watcherConfig) {
		cfg.heartbeatTimeout = heartbeatTimeout
	})
}

func WithWatcherGCTimeout(gcTimeout time.Duration) WatcherOption {
	return newFuncWatcherOption(func(cfg *watcherConfig) {
		cfg.gcTimeout = gcTimeout
	})
}

func WithWatcherRPCTimeout(rpcTimeout time.Duration) WatcherOption {
	return newFuncWatcherOption(func(cfg *watcherConfig) {
		cfg.rpcTimeout = rpcTimeout
	})
}
