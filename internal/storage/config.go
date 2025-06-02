package storage

import (
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultL0CompactionFileThreshold   = 500
	DefaultL0CompactionThreshold       = 4
	DefaultL0StopWritesThreshold       = 12
	DefaultL0TargetFileSize            = 2 << 20
	DefaultFlushSplitBytes             = 0
	DefaultLBaseMaxBytes               = 64 << 20
	DefaultMaxOpenFiles                = 1000
	DefaultMemTableSize                = 4 << 20
	DefaultMemTableStopWritesThreshold = 2
	DefaultMaxConcurrentCompactions    = 1
	DefaultMetricsLogInterval          = time.Duration(0)

	dataStoreDirName   = "_data"
	commitStoreDirName = "_commit"
)

type storeConfig struct {
	wal                         bool
	sync                        bool
	l0CompactionFileThreshold   int
	l0CompactionThreshold       int
	l0StopWritesThreshold       int
	l0TargetFileSize            int64
	flushSplitBytes             int64
	lbaseMaxBytes               int64
	maxOpenFiles                int
	memTableSize                int
	memTableStopWritesThreshold int
	maxConcurrentCompaction     int
}

func newStoreConfig(dbOpts ...StoreOption) (storeConfig, error) {
	cfg := storeConfig{
		wal:                         true,
		sync:                        true,
		l0CompactionFileThreshold:   DefaultL0CompactionFileThreshold,
		l0CompactionThreshold:       DefaultL0CompactionThreshold,
		l0StopWritesThreshold:       DefaultL0StopWritesThreshold,
		l0TargetFileSize:            DefaultL0TargetFileSize,
		flushSplitBytes:             DefaultFlushSplitBytes,
		lbaseMaxBytes:               DefaultLBaseMaxBytes,
		maxOpenFiles:                DefaultMaxOpenFiles,
		memTableSize:                DefaultMemTableSize,
		memTableStopWritesThreshold: DefaultMemTableStopWritesThreshold,
		maxConcurrentCompaction:     DefaultMaxConcurrentCompactions,
	}
	for _, dbOpt := range dbOpts {
		dbOpt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return storeConfig{}, err
	}
	return cfg, nil
}

func (cfg storeConfig) validate() error {
	if cfg.sync && !cfg.wal {
		return errors.New("storage: sync, but wal disabled")
	}
	return nil
}

type StoreOption interface {
	apply(*storeConfig)
}

type funcStoreOption struct {
	f func(*storeConfig)
}

func newFuncStoreOption(f func(*storeConfig)) *funcStoreOption {
	return &funcStoreOption{f: f}
}

func (fo *funcStoreOption) apply(cfg *storeConfig) {
	fo.f(cfg)
}

func WithWAL(wal bool) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.wal = wal
	})
}

func WithSync(sync bool) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.sync = sync
	})
}

func WithL0CompactionFileThreshold(l0CompactionFileThreshold int) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.l0CompactionFileThreshold = l0CompactionFileThreshold
	})
}

func WithL0CompactionThreshold(l0CompactionThreshold int) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.l0CompactionThreshold = l0CompactionThreshold
	})
}

func WithL0StopWritesThreshold(l0StopWritesThreshold int) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.l0StopWritesThreshold = l0StopWritesThreshold
	})
}

func WithL0TargetFileSize(l0TargetFileSize int64) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.l0TargetFileSize = l0TargetFileSize
	})
}

func WithFlushSplitBytes(flushSplitBytes int64) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.flushSplitBytes = flushSplitBytes
	})
}

func WithLBaseMaxBytes(lbaseMaxBytes int64) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.lbaseMaxBytes = lbaseMaxBytes
	})
}

func WithMaxOpenFiles(maxOpenFiles int) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.maxOpenFiles = maxOpenFiles
	})
}

func WithMemTableSize(memTableSize int) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.memTableSize = memTableSize
	})
}

func WithMemTableStopWritesThreshold(memTableStopWritesThreshold int) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.memTableStopWritesThreshold = memTableStopWritesThreshold
	})
}

func WithMaxConcurrentCompaction(maxConcurrentCompaction int) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.maxConcurrentCompaction = maxConcurrentCompaction
	})
}

type config struct {
	path               string
	dataStoreOptions   []StoreOption
	commitStoreOptions []StoreOption
	verbose            bool
	metricsLogInterval time.Duration
	trimDelay          time.Duration
	trimRateByte       int
	logger             *zap.Logger
	readOnly           bool
	cache              *Cache
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		metricsLogInterval: DefaultMetricsLogInterval,
		logger:             zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg config) validate() error {
	if len(cfg.path) == 0 {
		return errors.New("storage: no path")
	}
	if cfg.logger == nil {
		return errors.New("storage: no logger")
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

func WithPath(path string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.path = path
	})
}

func WithDataStoreOptions(dataStoreOpts ...StoreOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.dataStoreOptions = dataStoreOpts
	})
}

func WithCommitStoreOptions(commitStoreOpts ...StoreOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.commitStoreOptions = commitStoreOpts
	})
}

func WithVerboseLogging() Option {
	return newFuncOption(func(cfg *config) {
		cfg.verbose = true
	})
}

func WithMetricsLogInterval(metricsLogInterval time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.metricsLogInterval = metricsLogInterval
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}

// WithTrimDelay sets the delay before storage removes logs. If the value is
// zero, Trim will delay the removal of prefix log entries until writing
// additional log entries.
func WithTrimDelay(trimDelay time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.trimDelay = trimDelay
	})
}

// WithTrimRateByte is the Trim deletion speed in bytes per second. If the
// value is zero, Trim removes the log entries without throttling.
func WithTrimRateByte(trimRateByte int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.trimRateByte = trimRateByte
	})
}

// ReadOnly makes storage read-only. It is helpful only for testing. Usually,
// users do not have to call it.
func ReadOnly() Option {
	return newFuncOption(func(cfg *config) {
		cfg.readOnly = true
	})
}

// WithCache sets the cache for storage. Users can use the same cache across
// different storage. If the cache is not set, each storage uses its cache,
// which is 8 MB in size.
func WithCache(cache *Cache) Option {
	return newFuncOption(func(cfg *config) {
		cfg.cache = cache
	})
}

type readConfig struct {
	llsn types.LLSN
	glsn types.GLSN
}

func newReadConfig(opts []ReadOption) readConfig {
	cfg := readConfig{}
	for _, opt := range opts {
		opt.applyRead(&cfg)
	}
	return cfg
}

type ReadOption interface {
	applyRead(*readConfig)
}

type funcReadOption struct {
	f func(*readConfig)
}

func newFuncReadOption(f func(*readConfig)) *funcReadOption {
	return &funcReadOption{f: f}
}

func (fro *funcReadOption) applyRead(cfg *readConfig) {
	fro.f(cfg)
}

func AtGLSN(glsn types.GLSN) ReadOption {
	return newFuncReadOption(func(cfg *readConfig) {
		cfg.llsn = types.InvalidLLSN
		cfg.glsn = glsn
	})
}

func AtLLSN(llsn types.LLSN) ReadOption {
	return newFuncReadOption(func(cfg *readConfig) {
		cfg.llsn = llsn
		cfg.glsn = types.InvalidGLSN
	})
}
