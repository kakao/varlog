package varlogadm

import (
	"errors"
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

const (
	DefaultListenAddress     = "127.0.0.1:9090"
	DefaultWatcherRPCTimeout = 3 * time.Second

	DefaultClusterID                    = types.ClusterID(1)
	DefaultReplicationFactor            = 1
	DefaultInitialMRConnectRetryCount   = -1
	DefaultInitialMRConnectRetryBackoff = 100 * time.Millisecond
	DefaultMRConnTimeout                = 1 * time.Second
	DefaultMRCallTimeout                = 3 * time.Second

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

	mrManagerOptions []MRManagerOption
	watcherOptions   []WatcherOption
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

func WithMRManagerOptions(opts ...MRManagerOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.mrManagerOptions = opts
	})
}

func WithWatcherOptions(opts ...WatcherOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.watcherOptions = opts
	})
}

type mrManagerConfig struct {
	metadataRepositoryAddresses []string
	initialMRConnRetryCount     int
	initialMRConnRetryBackoff   time.Duration
	connTimeout                 time.Duration
	callTimeout                 time.Duration
}

func newMRManagerConfig(opts []MRManagerOption) (mrManagerConfig, error) {
	cfg := mrManagerConfig{
		initialMRConnRetryCount:   DefaultInitialMRConnectRetryCount,
		initialMRConnRetryBackoff: DefaultInitialMRConnectRetryBackoff,
		connTimeout:               DefaultMRConnTimeout,
		callTimeout:               DefaultMRCallTimeout,
	}
	for _, opt := range opts {
		opt.applyMRManager(&cfg)
	}
	return cfg, cfg.validate()
}

func (cfg *mrManagerConfig) validate() error {
	if len(cfg.metadataRepositoryAddresses) == 0 {
		return errors.New("no metadata repository address")
	}
	return nil
}

type MRManagerOption interface {
	applyMRManager(*mrManagerConfig)
}

type funcMRManagerOption struct {
	f func(*mrManagerConfig)
}

func newFuncMRManagerOption(f func(*mrManagerConfig)) *funcMRManagerOption {
	return &funcMRManagerOption{f: f}
}

func (fmo *funcMRManagerOption) applyMRManager(cfg *mrManagerConfig) {
	fmo.f(cfg)
}

func WithMetadataRepositoryAddress(addrs ...string) MRManagerOption {
	return newFuncMRManagerOption(func(cfg *mrManagerConfig) {
		cfg.metadataRepositoryAddresses = addrs
	})
}

func WithInitialMRConnRetryCount(retry int) MRManagerOption {
	return newFuncMRManagerOption(func(cfg *mrManagerConfig) {
		cfg.initialMRConnRetryCount = retry
	})
}

func WithInitialMRConnRetryBackoff(backoff time.Duration) MRManagerOption {
	return newFuncMRManagerOption(func(cfg *mrManagerConfig) {
		cfg.initialMRConnRetryBackoff = backoff
	})
}

func WithMRManagerConnTimeout(connTimeout time.Duration) MRManagerOption {
	return newFuncMRManagerOption(func(cfg *mrManagerConfig) {
		cfg.connTimeout = connTimeout
	})
}

func WithMRManagerCallTimeout(callTimeout time.Duration) MRManagerOption {
	return newFuncMRManagerOption(func(cfg *mrManagerConfig) {
		cfg.callTimeout = callTimeout
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
