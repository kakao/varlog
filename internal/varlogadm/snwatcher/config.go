package snwatcher

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/varlogadm/mrmanager"
	"github.com/kakao/varlog/internal/varlogadm/snmanager"
)

const (
	DefaultTick              = 100 * time.Millisecond
	DefaultReportInterval    = 10
	DefaultHeartbeatTimeout  = 10
	DefaultHeartbeatDeadline = 3 * time.Second
	DefaultReportDeadline    = 3 * time.Second
)

type config struct {
	eventHandler           EventHandler
	cmview                 mrmanager.ClusterMetadataView
	snmgr                  snmanager.StorageNodeManager
	tick                   time.Duration
	reportInterval         int
	heartbeatTimeout       int
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
		heartbeatCheckDeadline: DefaultHeartbeatDeadline,
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
	if cfg.tick == 0 {
		return fmt.Errorf("snwatcher: invalid tick %v", cfg.tick)
	}
	if cfg.heartbeatTimeout < 1 {
		return fmt.Errorf("snwatcher: invalid heartbeat timeout %d", cfg.heartbeatTimeout)
	}
	if cfg.reportInterval < 1 {
		return fmt.Errorf("snwatcher: invalid report interval %d", cfg.reportInterval)
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

// WithReportInterval sets the interval between each report in a unit of tick.
// It should be a positive number.
// If the tick is 1 second and the report interval is 10, the watcher reports
// metadata of storage nodes every 10 seconds.
func WithReportInterval(reportInterval int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.reportInterval = reportInterval
	})
}

// WithHeartbeatTimeout sets the heartbeat timeout, which is a unit of a tick,
// to decide whether a storage node is live.
// It should be a positive number.
// If the tick is 1 second and the heartbeat timeout is 10, the watcher decides
// that the storage node that has not responded over 10 seconds is failed.
func WithHeartbeatTimeout(heartbeatTimeout int) Option {
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
