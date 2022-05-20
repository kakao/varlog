package varlogadm

import (
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/varlogadm/mrmanager"
	"github.com/kakao/varlog/internal/varlogadm/snmanager"
	"github.com/kakao/varlog/internal/varlogadm/snwatcher"
	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultClusterID          = types.ClusterID(1)
	DefaultListenAddress      = "127.0.0.1:9090"
	DefaultReplicationFactor  = 1
	DefaultLogStreamGCTimeout = 24 * time.Hour
)

type config struct {
	cid                      types.ClusterID
	listenAddress            string
	replicationFactor        uint
	logStreamGCTimeout       time.Duration
	disableAutoLogStreamSync bool
	mrmgr                    mrmanager.MetadataRepositoryManager
	snmgr                    snmanager.StorageNodeManager
	snwatcherOpts            []snwatcher.Option
	logger                   *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		cid:                DefaultClusterID,
		listenAddress:      DefaultListenAddress,
		replicationFactor:  DefaultReplicationFactor,
		logStreamGCTimeout: DefaultLogStreamGCTimeout,
		logger:             zap.NewNop(),
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

func WithReplicationFactor(replicationFactor uint) Option {
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

func WithStorageNodeWatcherOptions(opts ...snwatcher.Option) Option {
	return newFuncOption(func(cfg *config) {
		cfg.snwatcherOpts = opts
	})
}
