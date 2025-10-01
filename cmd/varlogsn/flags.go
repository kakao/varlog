package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/units"
)

const (
	categoryStorage = "Storage:"
)

var (
	flagClusterID     = flags.ClusterID
	flagStorageNodeID = func() *cli.IntFlag {
		f := flags.GetStorageNodeIDFlag()
		f.Value = int(types.MinStorageNodeID)
		return f
	}()
	flagListen = flags.FlagDesc{
		Name:    "listen",
		Aliases: []string{"listen-address"},
		Envs:    []string{"LISTEN", "LISTEN_ADDRESS"},
	}
	flagAdvertise = flags.FlagDesc{
		Name:    "advertise",
		Aliases: []string{"advertise-address"},
		Envs:    []string{"ADVERTISE", "ADVERTISE_ADDRESS"},
	}
	flagBallastSize = flags.FlagDesc{
		Name:  "ballast-size",
		Envs:  []string{"BALLAST_SIZE"},
		Usage: "B, KiB, MiB, GiB",
	}
	flagVolumes = flags.FlagDesc{
		Name:    "volumes",
		Aliases: []string{"volume"},
		Envs:    []string{"VOLUMES", "VOLUME"},
	}

	flagMaxLogStreamReplicasCount = &cli.IntFlag{
		Name:  "max-logstream-replicas-count",
		Usage: "The maximum number of log stream replicas in a storage node, infinity if a negative value",
		Value: storagenode.DefaultMaxLogStreamReplicasCount,
	}

	flagAppendPipelineSize = &cli.IntFlag{
		Name:  "append-pipeline-size",
		Usage: "Append pipleline size",
		Value: storagenode.DefaultAppendPipelineSize,
		Action: func(_ *cli.Context, value int) error {
			if value < storagenode.MinAppendPipelineSize || value > storagenode.MaxAppendPipelineSize {
				return fmt.Errorf("invalid value \"%d\" for flag --append-pipeline-size", value)
			}
			return nil
		},
	}

	// flags for logstream executor options.
	flagLogStreamExecutorSequenceQueueCapacity = flags.FlagDesc{
		Name:    "logstream-executor-sequence-queue-capacity",
		Aliases: []string{"lse-sequence-queue-capacity"},
	}
	flagLogStreamExecutorWriteQueueCapacity = flags.FlagDesc{
		Name:    "logstream-executor-write-queue-capacity",
		Aliases: []string{"lse-write-queue-capacity"},
	}
	flagLogStreamExecutorCommitQueueCapacity = flags.FlagDesc{
		Name:    "logstream-executor-commit-queue-capacity",
		Aliases: []string{"lse-commit-queue-capacity"},
	}
	flagLogStreamExecutorReplicateclientQueueCapacity = flags.FlagDesc{
		Name:    "logstream-executor-replicate-client-queue-capacity",
		Aliases: []string{"lse-replicate-client-queue-capacity"},
	}

	// flags for storage.
	flagStorageCacheSize = &cli.StringFlag{
		Name:     "storage-cache-size",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_CACHE_SIZE"},
		Value:    units.ToByteSizeString(storage.DefaultCacheSize),
		Usage:    "Size of storage cache shared across all storages.",
	}

	flagStorageMetricsLogInterval = &cli.DurationFlag{
		Name:     "storage-metrics-log-interval",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_METRICS_LOG_INTERVAL"},
		Value:    storage.DefaultMetricsLogInterval,
	}
	flagStorageValueStore = &cli.GenericFlag{
		Name:     "storage-value-store",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_VALUE_STORE"},
		Value: func() cli.Generic {
			s := &StorageStoreSetting{}
			s.init()
			return s
		}(),
		Usage: "Storage value store options in the format <key>=<value>,<key>=<value>,... Example: --storage-value-store \"wal=true,sync_wal=true,l0_target_file_size=64MiB\"",
	}
	flagStorageCommitStore = &cli.GenericFlag{
		Name:     "storage-commit-store",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_COMMIT_STORE"},
		Value: func() cli.Generic {
			s := &StorageStoreSetting{}
			s.init()
			return s
		}(),
		Usage: "Storage commit store options in the format <key>=<value>,<key>=<value>,... Example: --storage-commit-store \"wal=true,sync_wal=true,l0_target_file_size=64MiB\"",
	}
)

type StorageStoreSetting struct {
	wal             bool
	syncWAL         bool
	walBytesPerSync int64

	sstBytesPerSync int64

	memTableSize                int64
	memTableStopWritesThreshold int
	flushSplitBytes             int64

	l0CompactionFileThreshold int
	l0CompactionThreshold     int
	l0StopWritesThreshold     int
	l0TargetFileSize          int64

	lbaseMaxBytes int64

	maxConcurrentCompactions int

	trimDelay time.Duration
	trimRate  int64

	maxOpenFiles int

	enableTelemetry bool

	verbose bool
}

var _ cli.Generic = (*StorageStoreSetting)(nil)

func (setting *StorageStoreSetting) init() {
	*setting = StorageStoreSetting{
		wal:                         true,
		syncWAL:                     true,
		walBytesPerSync:             storage.DefaultWALBytesPerSync,
		sstBytesPerSync:             storage.DefaultSSTBytesPerSync,
		memTableSize:                storage.DefaultMemTableSize,
		memTableStopWritesThreshold: storage.DefaultMemTableStopWritesThreshold,
		l0CompactionFileThreshold:   storage.DefaultL0CompactionFileThreshold,
		l0CompactionThreshold:       storage.DefaultL0CompactionThreshold,
		l0StopWritesThreshold:       storage.DefaultL0StopWritesThreshold,
		l0TargetFileSize:            storage.DefaultL0TargetFileSize,
		lbaseMaxBytes:               storage.DefaultLBaseMaxBytes,
		maxConcurrentCompactions:    storage.DefaultMaxConcurrentCompactions,
		maxOpenFiles:                storage.DefaultMaxOpenFiles,
		enableTelemetry:             false,
		verbose:                     false,
	}
}

func (setting *StorageStoreSetting) Set(value string) (err error) {
	parts := strings.SplitSeq(value, ",")
	for part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid storage store format: %s, expected <key>=<value>", part)
		}
		k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])

		switch strings.ToLower(k) {
		case "wal":
			setting.wal, err = strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("invalid value for wal: %w", err)
			}
		case "sync_wal":
			setting.syncWAL, err = strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("invalid value for sync_wal: %w", err)
			}
		case "wal_bytes_per_sync":
			setting.walBytesPerSync, err = units.FromByteSizeString(v)
			if err != nil {
				return fmt.Errorf("invalid value for wal_bytes_per_sync: %w", err)
			}
		case "sst_bytes_per_sync", "bytes_per_sync":
			setting.sstBytesPerSync, err = units.FromByteSizeString(v)
			if err != nil {
				return fmt.Errorf("invalid value for sst_bytes_per_sync: %w", err)
			}
		case "mem_table_size":
			setting.memTableSize, err = units.FromByteSizeString(v)
			if err != nil {
				return fmt.Errorf("invalid value for mem_table_size: %w", err)
			}
		case "mem_table_stop_writes_threshold":
			setting.memTableStopWritesThreshold, err = strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("invalid value for mem_table_stop_writes_threshold: %w", err)
			}
		case "flush_split_bytes":
			setting.flushSplitBytes, err = units.FromByteSizeString(v)
			if err != nil {
				return fmt.Errorf("invalid value for flush_split_bytes: %w", err)
			}
		case "l0_compaction_file_threshold":
			setting.l0CompactionFileThreshold, err = strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("invalid value for l0_compaction_file_threshold: %w", err)
			}
		case "l0_compaction_threshold":
			setting.l0CompactionThreshold, err = strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("invalid value for l0_compaction_threshold: %w", err)
			}
		case "l0_stop_writes_threshold":
			setting.l0StopWritesThreshold, err = strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("invalid value for l0_stop_writes_threshold: %w", err)
			}
		case "l0_target_file_size":
			setting.l0TargetFileSize, err = units.FromByteSizeString(v)
			if err != nil {
				return fmt.Errorf("invalid value for l0_target_file_size: %w", err)
			}
		case "lbase_max_bytes":
			setting.lbaseMaxBytes, err = units.FromByteSizeString(v)
			if err != nil {
				return fmt.Errorf("invalid value for lbase_max_bytes: %w", err)
			}
		case "max_concurrent_compactions":
			setting.maxConcurrentCompactions, err = strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("invalid value for max_concurrent_compactions: %w", err)
			}
		case "flush_delay_delete_range", "trim_delay":
			setting.trimDelay, err = time.ParseDuration(v)
			if err != nil {
				return fmt.Errorf("invalid value for trim_delay: %w", err)
			}
		case "target_byte_deletion_rate", "trim_rate":
			setting.trimRate, err = units.FromByteSizeString(v)
			if err != nil {
				return fmt.Errorf("invalid value for trim_rate: %w", err)
			}
		case "max_open_files":
			setting.maxOpenFiles, err = strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("invalid value for max_open_files: %w", err)
			}
		case "enable_telemetry":
			setting.enableTelemetry, err = strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("invalid value for enable_telemetry: %w", err)
			}
		case "verbose":
			setting.verbose, err = strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("invalid value for verbose: %w", err)
			}
		default:
			return fmt.Errorf("unknown key in storage store options: %s", k)
		}
	}

	return nil
}

func (setting *StorageStoreSetting) String() string {
	var opts []string

	if setting.wal {
		opts = append(opts, "wal=true")
	}
	if setting.syncWAL {
		opts = append(opts, "sync_wal=true")
	}
	if setting.walBytesPerSync > 0 {
		opts = append(opts, fmt.Sprintf("wal_bytes_per_sync=%s", units.ToByteSizeString(float64(setting.walBytesPerSync))))
	}
	if setting.sstBytesPerSync > 0 {
		opts = append(opts, fmt.Sprintf("sst_bytes_per_sync=%s", units.ToByteSizeString(float64(setting.sstBytesPerSync))))
	}
	if setting.memTableSize > 0 {
		opts = append(opts, fmt.Sprintf("mem_table_size=%s", units.ToByteSizeString(float64(setting.memTableSize))))
	}
	if setting.memTableStopWritesThreshold > 0 {
		opts = append(opts, fmt.Sprintf("mem_table_stop_writes_threshold=%d", setting.memTableStopWritesThreshold))
	}
	if setting.flushSplitBytes > 0 {
		opts = append(opts, fmt.Sprintf("flush_split_bytes=%s", units.ToByteSizeString(float64(setting.flushSplitBytes))))
	}
	if setting.l0CompactionFileThreshold > 0 {
		opts = append(opts, fmt.Sprintf("l0_compaction_file_threshold=%d", setting.l0CompactionFileThreshold))
	}
	if setting.l0CompactionThreshold > 0 {
		opts = append(opts, fmt.Sprintf("l0_compaction_threshold=%d", setting.l0CompactionThreshold))
	}
	if setting.l0StopWritesThreshold > 0 {
		opts = append(opts, fmt.Sprintf("l0_stop_writes_threshold=%d", setting.l0StopWritesThreshold))
	}
	if setting.l0TargetFileSize > 0 {
		opts = append(opts, fmt.Sprintf("l0_target_file_size=%s", units.ToByteSizeString(float64(setting.l0TargetFileSize))))
	}
	if setting.lbaseMaxBytes > 0 {
		opts = append(opts, fmt.Sprintf("lbase_max_bytes=%s", units.ToByteSizeString(float64(setting.lbaseMaxBytes))))
	}
	if setting.maxConcurrentCompactions > 0 {
		opts = append(opts, fmt.Sprintf("max_concurrent_compactions=%d", setting.maxConcurrentCompactions))
	}
	if setting.trimDelay > 0 {
		opts = append(opts, fmt.Sprintf("trim_delay=%s", setting.trimDelay.String()))
	}
	if setting.trimRate > 0 {
		opts = append(opts, fmt.Sprintf("trim_rate=%s", units.ToByteSizeString(float64(setting.trimRate))))
	}
	if setting.maxOpenFiles > 0 {
		opts = append(opts, fmt.Sprintf("max_open_files=%d", setting.maxOpenFiles))
	}
	if setting.enableTelemetry {
		opts = append(opts, "enable_telemetry=true")
	}
	if setting.verbose {
		opts = append(opts, "verbose=true")
	}

	return strings.Join(opts, ",")
}
