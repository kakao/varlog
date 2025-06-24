package storage

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"go.uber.org/zap"
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
	DefaultWALBytesPerSync             = 0
	DefaultSSTBytesPerSync             = 512 << 10
)

type store struct {
	storeConfig

	db        *pebble.DB
	writeOpts *pebble.WriteOptions
}

func newStore(path string, opts ...StoreOption) (*store, error) {
	cfg, err := newStoreConfig(opts...)
	if err != nil {
		return nil, err
	}

	s := &store{
		storeConfig: cfg,
	}
	s.writeOpts = &pebble.WriteOptions{Sync: cfg.syncWAL}

	pebbleOpts := &pebble.Options{
		Cache:                       s.cache.get(),
		DisableWAL:                  !s.wal,
		WALBytesPerSync:             s.walBytesPerSync,
		BytesPerSync:                s.sstBytesPerSync,
		L0CompactionFileThreshold:   s.l0CompactionFileThreshold,
		L0CompactionThreshold:       s.l0CompactionThreshold,
		L0StopWritesThreshold:       s.l0StopWritesThreshold,
		LBaseMaxBytes:               s.lbaseMaxBytes,
		MaxOpenFiles:                s.maxOpenFiles,
		MemTableSize:                uint64(s.memTableSize),
		MemTableStopWritesThreshold: s.memTableStopWritesThreshold,
		MaxConcurrentCompactions:    func() int { return s.maxConcurrentCompaction },
		Levels:                      make([]pebble.LevelOptions, 7),
		ErrorIfExists:               false,
		FlushDelayDeleteRange:       s.trimDelay,
		TargetByteDeletionRate:      s.trimRateByte,
		FormatMajorVersion:          pebble.FormatVirtualSSTables,
	}
	pebbleOpts.Levels[0].TargetFileSize = cfg.l0TargetFileSize
	for i := range pebbleOpts.Levels {
		l := &pebbleOpts.Levels[i]
		l.BlockSize = 32 << 10
		l.IndexBlockSize = 256 << 10
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = pebbleOpts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	pebbleOpts.Levels[6].FilterPolicy = nil
	pebbleOpts.FlushSplitBytes = s.flushSplitBytes
	pebbleOpts.EnsureDefaults()

	el := pebble.MakeLoggingEventListener(newLogAdaptor(s.logger))
	pebbleOpts.EventListener = &el
	// Enabled events:
	//  - BackgroundError
	//  - DiskSlow
	//  - FormatUpgrade
	//  - WriteStallBegin
	//  - WriteStallEnd
	pebbleOpts.EventListener.FlushBegin = nil
	pebbleOpts.EventListener.FlushEnd = nil
	pebbleOpts.EventListener.ManifestCreated = nil
	pebbleOpts.EventListener.ManifestDeleted = nil
	pebbleOpts.EventListener.TableCreated = nil
	pebbleOpts.EventListener.TableDeleted = nil
	pebbleOpts.EventListener.TableIngested = nil
	pebbleOpts.EventListener.TableStatsLoaded = nil
	pebbleOpts.EventListener.TableValidated = nil
	pebbleOpts.EventListener.WALCreated = nil
	pebbleOpts.EventListener.WALDeleted = nil
	if !s.verbose {
		pebbleOpts.EventListener.CompactionBegin = nil
		pebbleOpts.EventListener.CompactionEnd = nil
	}

	if s.readOnly {
		pebbleOpts.ReadOnly = true
	}
	var sb strings.Builder
	sb.WriteString("opening database: path=")
	sb.WriteString(path)
	sb.WriteString(", wal=")
	sb.WriteString(strconv.FormatBool(s.wal))
	sb.WriteString(", wal_sync=")
	sb.WriteString(strconv.FormatBool(s.syncWAL))
	if s.verbose {
		sb.WriteString("\n")
		sb.WriteString(pebbleOpts.String())
	}
	s.logger.Info(sb.String())

	s.db, err = pebble.Open(path, pebbleOpts)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *store) close() error {
	return s.db.Close()
}

type telemetryConfig struct {
	metricRecorder MetricRecorder
	enable         bool
}

type storeConfig struct {
	wal                         bool
	syncWAL                     bool
	walBytesPerSync             int
	sstBytesPerSync             int
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
	cache                       *Cache
	trimDelay                   time.Duration
	trimRateByte                int
	readOnly                    bool
	logger                      *zap.Logger
	verbose                     bool

	telemetryConfig telemetryConfig
}

func newStoreConfig(dbOpts ...StoreOption) (storeConfig, error) {
	cfg := storeConfig{
		wal:                         true,
		syncWAL:                     true,
		walBytesPerSync:             DefaultWALBytesPerSync,
		sstBytesPerSync:             DefaultSSTBytesPerSync,
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
		telemetryConfig: telemetryConfig{
			metricRecorder: defaultMetricRecorder,
			enable:         false,
		},
	}
	for _, dbOpt := range dbOpts {
		dbOpt.applyStore(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return storeConfig{}, err
	}
	return cfg, nil
}

func (cfg storeConfig) validate() error {
	if cfg.syncWAL && !cfg.wal {
		return errors.New("storage: sync, but wal disabled")
	}
	return nil
}

type StoreOption interface {
	applyStore(*storeConfig)
}

type funcStoreOption struct {
	f func(*storeConfig)
}

func newFuncStoreOption(f func(*storeConfig)) *funcStoreOption {
	return &funcStoreOption{f: f}
}

func (fo *funcStoreOption) applyStore(cfg *storeConfig) {
	fo.f(cfg)
}

func WithWAL(wal bool) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.wal = wal
	})
}

// WithSyncWAL sets whether write operations are synchronized to disk by
// waiting for the Pebble WAL (Write-Ahead Log) to be flushed to durable
// storage.
//
// Enabling this option ensures each write is durable, but may reduce write
// performance. Disabling it can improve performance but risks recent writes
// being lost if a process or machine crash occurs, as data may only be
// buffered in memory.
func WithSyncWAL(syncWAL bool) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.syncWAL = syncWAL
	})
}

// WithWALBytesPerSync sets the number of bytes to write to the WAL
// (Write-Ahead Log) before triggering a background sync. This helps smooth out
// disk write latencies and avoids large, sudden disk writes. This option maps
// to Pebble's WALBytesPerSync. The default value is 0 (no background syncing).
func WithWALBytesPerSync(walBytesPerSync int) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.walBytesPerSync = walBytesPerSync
	})
}

// WithSSTBytesperSync sets the number of bytes to write to SSTables before
// triggering a background sync. This helps avoid latency spikes caused by the
// OS flushing large amounts of dirty data at once. This option only affects
// SSTable syncs and maps to Pebble's BytesPerSync. The default value is 512KB.
func WithSSTBytesperSync(sstBytesPerSync int) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.sstBytesPerSync = sstBytesPerSync
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

// WithCache sets the cache for the store. Users can share the same cache
// across multiple stores. If not set, each store uses its own 8 MB cache by
// default.
func WithCache(cache *Cache) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.cache = cache
	})
}

// ReadOnly sets the store to read-only mode. This is mainly useful for
// testing. Most users do not need to call this option.
func ReadOnly(readOnly bool) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.readOnly = readOnly
	})
}

// WithTrimDelay sets the delay before the store removes data. If set to zero,
// Trim will delay removal until additional data is written.
func WithTrimDelay(trimDelay time.Duration) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.trimDelay = trimDelay
	})
}

// WithTrimRateByte sets the Trim deletion speed in bytes per second. If set to
// zero, Trim removes data without throttling.
func WithTrimRateByte(trimRateByte int) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.trimRateByte = trimRateByte
	})
}

// WithVerbose enables or disables verbose mode for the store. When enabled,
// the store logs compaction events.
func WithVerbose(verbose bool) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.verbose = verbose
	})
}

func withLogger(logger *zap.Logger) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.logger = logger
	})
}

// EnableTelemetry enables or disables telemetry for the store. When disabled,
// even if WithMetricRecorder is set, no metrics will be recorded. It is
// disabled by default.
func EnableTelemetry(enableTelemetry bool) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.telemetryConfig.enable = enableTelemetry
	})
}

// WithMetricRecorder sets the MetricRecorder for the store. If telemetry is
// disabled via EnableTelemetry(false), the provided MetricRecorder will not
// record metrics. This option allows integration with custom metrics backends.
func WithMetricRecorder(metricRecorder MetricRecorder) StoreOption {
	return newFuncStoreOption(func(cfg *storeConfig) {
		cfg.telemetryConfig.metricRecorder = metricRecorder
	})
}
