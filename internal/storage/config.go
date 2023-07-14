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

	dataDBDirName   = "_data"
	commitDBDirName = "_commit"
)

type dbConfig struct {
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

func newDBConfig(dbOpts ...DBOption) dbConfig {
	cfg := dbConfig{
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
	return cfg
}

type DBOption interface {
	apply(*dbConfig)
}

type funcDBOption struct {
	f func(*dbConfig)
}

func newFuncDBOption(f func(*dbConfig)) *funcDBOption {
	return &funcDBOption{f: f}
}

func (fo *funcDBOption) apply(cfg *dbConfig) {
	fo.f(cfg)
}

func WithL0CompactionFileThreshold(l0CompactionFileThreshold int) DBOption {
	return newFuncDBOption(func(cfg *dbConfig) {
		cfg.l0CompactionFileThreshold = l0CompactionFileThreshold
	})
}

func WithL0CompactionThreshold(l0CompactionThreshold int) DBOption {
	return newFuncDBOption(func(cfg *dbConfig) {
		cfg.l0CompactionThreshold = l0CompactionThreshold
	})
}

func WithL0StopWritesThreshold(l0StopWritesThreshold int) DBOption {
	return newFuncDBOption(func(cfg *dbConfig) {
		cfg.l0StopWritesThreshold = l0StopWritesThreshold
	})
}

func WithL0TargetFileSize(l0TargetFileSize int64) DBOption {
	return newFuncDBOption(func(cfg *dbConfig) {
		cfg.l0TargetFileSize = l0TargetFileSize
	})
}

func WithFlushSplitBytes(flushSplitBytes int64) DBOption {
	return newFuncDBOption(func(cfg *dbConfig) {
		cfg.flushSplitBytes = flushSplitBytes
	})
}

func WithLBaseMaxBytes(lbaseMaxBytes int64) DBOption {
	return newFuncDBOption(func(cfg *dbConfig) {
		cfg.lbaseMaxBytes = lbaseMaxBytes
	})
}

func WithMaxOpenFiles(maxOpenFiles int) DBOption {
	return newFuncDBOption(func(cfg *dbConfig) {
		cfg.maxOpenFiles = maxOpenFiles
	})
}

func WithMemTableSize(memTableSize int) DBOption {
	return newFuncDBOption(func(cfg *dbConfig) {
		cfg.memTableSize = memTableSize
	})
}

func WithMemTableStopWritesThreshold(memTableStopWritesThreshold int) DBOption {
	return newFuncDBOption(func(cfg *dbConfig) {
		cfg.memTableStopWritesThreshold = memTableStopWritesThreshold
	})
}

func WithMaxConcurrentCompaction(maxConcurrentCompaction int) DBOption {
	return newFuncDBOption(func(cfg *dbConfig) {
		cfg.maxConcurrentCompaction = maxConcurrentCompaction
	})
}

type config struct {
	path               string
	wal                bool
	sync               bool
	separateDB         bool
	dataDBConfig       dbConfig
	commitDBConfig     dbConfig
	verbose            bool
	metricsLogInterval time.Duration
	trimDelay          time.Duration
	logger             *zap.Logger
	readOnly           bool
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		wal:                true,
		sync:               true,
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
	if cfg.sync && !cfg.wal {
		return errors.New("storage: sync, but wal disabled")
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

func SeparateDatabase() Option {
	return newFuncOption(func(cfg *config) {
		cfg.separateDB = true
	})
}

func WithoutSync() Option {
	return newFuncOption(func(cfg *config) {
		cfg.sync = false
	})
}

func WithoutWAL() Option {
	return newFuncOption(func(cfg *config) {
		cfg.wal = false
	})
}

func WithDataDBOptions(dataDBOpts ...DBOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.dataDBConfig = newDBConfig(dataDBOpts...)
	})
}

func WithCommitDBOptions(commitDBOpts ...DBOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.commitDBConfig = newDBConfig(commitDBOpts...)
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

// ReadOnly makes storage read-only. It is helpful only for testing. Usually,
// users do not have to call it.
func ReadOnly() Option {
	return newFuncOption(func(cfg *config) {
		cfg.readOnly = true
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
