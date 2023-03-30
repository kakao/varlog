package storage

import (
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultL0CompactionThreshold       = 4
	DefaultL0StopWritesThreshold       = 12
	DefaultLBaseMaxBytes               = 64 << 20
	DefaultMaxOpenFiles                = 1000
	DefaultMemTableSize                = 4 << 20
	DefaultMemTableStopWritesThreshold = 2
	DefaultMaxConcurrentCompactions    = 1
	DefaultMetricsLogInterval          = time.Duration(0)
)

type config struct {
	path string
	wal  bool
	sync bool

	l0CompactionThreshold       int
	l0StopWritesThreshold       int
	lbaseMaxBytes               int64
	maxOpenFiles                int
	memTableSize                int
	memTableStopWritesThreshold int
	maxConcurrentCompaction     int
	verbose                     bool
	metricsLogInterval          time.Duration
	logger                      *zap.Logger

	readOnly bool
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		wal:  true,
		sync: true,

		l0CompactionThreshold: DefaultL0CompactionThreshold,
		l0StopWritesThreshold: DefaultL0StopWritesThreshold,
		lbaseMaxBytes:         DefaultLBaseMaxBytes,

		maxOpenFiles:                DefaultMaxOpenFiles,
		memTableSize:                DefaultMemTableSize,
		memTableStopWritesThreshold: DefaultMemTableStopWritesThreshold,
		maxConcurrentCompaction:     DefaultMaxConcurrentCompactions,

		metricsLogInterval: DefaultMetricsLogInterval,
		logger:             zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg, cfg.validate()
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

func WithL0CompactionThreshold(l0CompactionThreshold int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.l0CompactionThreshold = l0CompactionThreshold
	})
}

func WithL0StopWritesThreshold(l0StopWritesThreshold int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.l0StopWritesThreshold = l0StopWritesThreshold
	})
}

func WithLBaseMaxBytes(lbaseMaxBytes int64) Option {
	return newFuncOption(func(cfg *config) {
		cfg.lbaseMaxBytes = lbaseMaxBytes
	})
}

func WithMaxOpenFiles(maxOpenFiles int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.maxOpenFiles = maxOpenFiles
	})
}

func WithMemTableSize(memTableSize int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.memTableSize = memTableSize
	})
}

func WithMemTableStopWritesThreshold(memTableStopWritesThreshold int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.memTableStopWritesThreshold = memTableStopWritesThreshold
	})
}

func WithMaxConcurrentCompaction(maxConcurrentCompaction int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.maxConcurrentCompaction = maxConcurrentCompaction
	})
}

func WithVerboseLogging() Option {
	return newFuncOption(func(cfg *config) {
		cfg.verbose = true
	})
}

func WithMetrisLogInterval(metricsLogInterval time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.metricsLogInterval = metricsLogInterval
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}

// ReadOnly makes storage read-only. It is helpful only for testing. Usually,
// users do not have to call it.
func ReadOnly() Option {
	return newFuncOption(func(cfg *config) {
		cfg.readOnly = true
	})
}

/*
func errInvalidLevelOptions(kv string, err error) error {
	return fmt.Errorf("storage: level options: invalid option %s: %w", kv, err)
}

func parseLevelOptionsList(str string) ([]pebble.LevelOptions, error) {
	strList := strings.Split(str, ";")
	optsList := make([]pebble.LevelOptions, 0, len(strList))
	for _, optsStr := range strList {
		opts, err := parseLevelOptions(optsStr)
		if err != nil {
			return nil, err
		}
		optsList = append(optsList, opts)
	}
	return optsList, nil
}

func parseLevelOptions(str string) (opts pebble.LevelOptions, err error) {
	opts.EnsureDefaults()
	for _, str := range strings.Split(str, ",") {
		kv := strings.Split(str, "=")
		if len(kv) != 2 {
			return opts, fmt.Errorf("storage: level options: invalid option %s", str)
		}
		key, value := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		switch key {
		case "block_restart_interval":
			opts.BlockRestartInterval, err = strconv.Atoi(value)
		case "block_size":
			opts.BlockSize, err = strconv.Atoi(value)
		case "compression":
			switch value {
			case "Default":
				opts.Compression = pebble.DefaultCompression
			case "NoCompression":
				opts.Compression = pebble.NoCompression
			case "Snappy":
				opts.Compression = pebble.SnappyCompression
			case "ZSTD":
				opts.Compression = pebble.ZstdCompression
			default:
				return opts, fmt.Errorf("storage: level options: unknown compression: %s", value)
			}
		case "filter_policy":
			var filterPolicy int
			filterPolicy, err = strconv.Atoi(value)
			if err == nil {
				opts.FilterPolicy = bloom.FilterPolicy(filterPolicy)
			}
		case "filter_type":
			switch value {
			case "table":
				opts.FilterType = pebble.TableFilter
			default:
				return opts, fmt.Errorf("storage: level options: unknown filter type: %s", value)
			}
		case "index_block_size":
			opts.IndexBlockSize, err = strconv.Atoi(value)
		case "target_file_size":
			opts.TargetFileSize, err = strconv.ParseInt(value, 10, 64)
		default:
			return opts, fmt.Errorf("stroage: level options: unknown option %s", str)
		}
		if err != nil {
			return opts, errInvalidLevelOptions(str, err)
		}
	}
	return opts, nil
}
*/

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
