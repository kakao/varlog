package snwatcher

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/internal/admin/snmanager"
	"github.com/kakao/varlog/internal/admin/stats"
)

const (
	DefaultTick                   = 1 * time.Second
	DefaultReportInterval         = 5 * DefaultTick
	DefaultHeartbeatTimeout       = 5 * DefaultTick
	DefaultHeartbeatCheckDeadline = DefaultTick
	DefaultReportDeadline         = DefaultTick
)

type config struct {
	eventHandler           EventHandler
	cmview                 mrmanager.ClusterMetadataView
	snmgr                  snmanager.StorageNodeManager
	statsRepos             stats.Repository
	tick                   time.Duration
	reportInterval         time.Duration
	heartbeatTimeout       time.Duration
	heartbeatCheckDeadline time.Duration
	reportDeadline         time.Duration

	logger *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		tick:                   DefaultTick,
		reportInterval:         DefaultReportInterval,
		reportDeadline:         DefaultReportDeadline,
		heartbeatTimeout:       DefaultHeartbeatTimeout,
		heartbeatCheckDeadline: DefaultHeartbeatCheckDeadline,
		logger:                 zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	cfg.logger = cfg.logger.Named("snwatcher")
	return cfg, nil
}

func (cfg *config) validate() error {
	if cfg.cmview == nil {
		return errors.New("snwatcher: cluster metadata view is nil")
	}
	if cfg.snmgr == nil {
		return errors.New("snwatcher: storage node manager is nil")
	}
	if cfg.eventHandler == nil {
		return errors.New("snwatcher: event handler is nil")
	}
	if cfg.statsRepos == nil {
		return errors.New("snwatcher: stats repository is nil")
	}
	if cfg.tick == time.Duration(0) {
		return fmt.Errorf("snwatcher: invalid tick %v", cfg.tick)
	}
	if cfg.heartbeatTimeout < cfg.tick {
		return fmt.Errorf("snwatcher: invalid heartbeat timeout %d, it should be lagger than tick %d",
			cfg.heartbeatTimeout, cfg.tick)
	}
	if cfg.reportInterval < cfg.tick {
		return fmt.Errorf("snwatcher: invalid report interval %d, it should be lagger than tick %d",
			cfg.reportInterval, cfg.tick)
	}
	if cfg.logger == nil {
		return errors.New("snwatcher: logger is nil")
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

func WithStorageNodeWatcherHandler(eventHandler EventHandler) Option {
	return newFuncOption(func(cfg *config) {
		cfg.eventHandler = eventHandler
	})
}

func WithClusterMetadataView(cmview mrmanager.ClusterMetadataView) Option {
	return newFuncOption(func(cfg *config) {
		cfg.cmview = cmview
	})
}

func WithStorageNodeManager(snmgr snmanager.StorageNodeManager) Option {
	return newFuncOption(func(cfg *config) {
		cfg.snmgr = snmgr
	})
}

func WithStatisticsRepository(statsRepos stats.Repository) Option {
	return newFuncOption(func(cfg *config) {
		cfg.statsRepos = statsRepos
	})
}

// WithTick sets tick interval of storage node watcher.
// It fetches cluster metadata from ClusterMetadataView and metadata from all
// storage nodes at each tick.
// It can make overload to cluster if the tick is too small. On the other hand,
// it can be too slow to handle the failure of nodes if the tick is too large.
func WithTick(tick time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.tick = tick
	})
}

// WithReportInterval sets the interval between each report.
// It should be largger than tick.
func WithReportInterval(reportInterval time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.reportInterval = reportInterval
	})
}

// WithHeartbeatTimeout sets the heartbeat timeout
// to decide whether a storage node is live.
// It should be largger than tick.
func WithHeartbeatTimeout(heartbeatTimeout time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.heartbeatTimeout = heartbeatTimeout
	})
}

// WithHeartbeatCheckDeadline sets a deadline that the watcher checks storage
// nodes and handles failed storage nodes.
// If it is too small, the watcher cannot complete checking the heartbeat of
// all storage nodes in the cluster.
func WithHeartbeatCheckDeadline(heartbeatCheckTimeout time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.heartbeatCheckDeadline = heartbeatCheckTimeout
	})
}

// WithReportDeadline sets a deadline that the watcher fetches metadata of
// storage nodes and reports them by using HandleReport.
// If it is too small, the watcher cannot complete reporting the metadata of
// all storage nodes in the cluster.
func WithReportDeadline(reportDeadline time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.reportDeadline = reportDeadline
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}
