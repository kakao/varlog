package main

import (
	"fmt"

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
	flagStorageDataDBDisableWAL = &cli.BoolFlag{
		Name:     "storage-datadb-disable-wal",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_DATADB_DISABLE_WAL"},
		Usage:    "Disable the Write-Ahead Logging (WAL) for the data database in storage. If --experimental-storage-separate-db is not used, this setting applies to both the data and commit databases.",
	}
	flagStorageDataDBNoSync = &cli.BoolFlag{
		Name:     "storage-datadb-no-sync",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_DATADB_NO_SYNC"},
		Usage:    "Disable synchronization for the data database in storage. If true, written data might be lost on process termination. If --experimental-storage-separate-db is not used, this setting applies to both the data and commit databases.",
	}

	flagStorageCommitDBDisableWAL = &cli.BoolFlag{
		Name:     "storage-commitdb-disable-wal",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_COMMITDB_DISABLE_WAL"},
		Usage:    "Disable the Write-Ahead Logging (WAL) for the commit database in storage. If --experimental-storage-separate-db is not used, this setting is ignored.",
	}
	flagStorageCommitDBNoSync = &cli.BoolFlag{
		Name:     "storage-commitdb-no-sync",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_COMMITDB_NO_SYNC"},
		Usage:    "Disable synchronization for the commit database in storage. If true, written data might be lost on process termination. If --experimental-storage-separate-db is not used, this setting is ignored.",
	}

	flagStorageVerbose = &cli.BoolFlag{
		Name:     "storage-verbose",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_VERBOSE"},
	}
	flagStorageCacheSize = &cli.StringFlag{
		Name:     "storage-cache-size",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_CACHE_SIZE"},
		Value:    units.ToByteSizeString(storage.DefaultCacheSize),
		Usage:    "Size of storage cache shared across all storages.",
	}

	flagStorageL0CompactionFileThreshold = &cli.IntSliceFlag{
		Name:     "storage-l0-compaction-file-threshold",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_L0_COMPACTION_FILE_THRESHOLD"},
		Value:    cli.NewIntSlice(storage.DefaultL0CompactionFileThreshold),
		Usage:    `L0CompactionFileThreshold of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageL0CompactionThreshold = &cli.IntSliceFlag{
		Name:     "storage-l0-compaction-threshold",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_L0_COMPACTION_THRESHOLD"},
		Value:    cli.NewIntSlice(storage.DefaultL0CompactionThreshold),
		Usage:    `L0CompactionThreshold of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageL0StopWritesThreshold = &cli.IntSliceFlag{
		Name:     "storage-l0-stop-writes-threshold",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_L0_STOP_WRITES_THRESHOLD"},
		Value:    cli.NewIntSlice(storage.DefaultL0StopWritesThreshold),
		Usage:    `L0StopWritesThreshold of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageL0TargetFileSize = &cli.StringSliceFlag{
		Name:     "storage-l0-target-file-size",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_L0_TARGET_FILE_SIZE"},
		Value:    cli.NewStringSlice(units.ToByteSizeString(storage.DefaultL0TargetFileSize)),
		Usage:    `L0TargetFileSize of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageFlushSplitBytes = &cli.StringSliceFlag{
		Name:     "storage-flush-split-bytes",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_FLUSH_SPLIT_BYTES"},
		Value:    cli.NewStringSlice(units.ToByteSizeString(storage.DefaultFlushSplitBytes)),
		Usage:    `FlushSplitBytes of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageLBaseMaxBytes = &cli.StringSliceFlag{
		Name:     "storage-lbase-max-bytes",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_LBASE_MAX_BYTES"},
		Value:    cli.NewStringSlice(units.ToByteSizeString(storage.DefaultLBaseMaxBytes)),
		Usage:    `LBaseMaxBytes of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageMaxOpenFiles = &cli.IntSliceFlag{
		Name:     "storage-max-open-files",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_MAX_OPEN_FILES"},
		Value:    cli.NewIntSlice(storage.DefaultMaxOpenFiles),
		Usage:    `MaxOpenFiles of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageMemTableSize = &cli.StringSliceFlag{
		Name:     "storage-mem-table-size",
		Category: categoryStorage,
		Aliases:  []string{"storage-memtable-size"},
		EnvVars:  []string{"STORAGE_MEM_TABLE_SIZE", "STORAGE_MEMTABLE_SIZE"},
		Value:    cli.NewStringSlice(units.ToByteSizeString(storage.DefaultMemTableSize)),
		Usage:    `MemTableSize of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageMemTableStopWritesThreshold = &cli.IntSliceFlag{
		Name:     "storage-mem-table-stop-writes-threshold",
		Category: categoryStorage,
		Aliases:  []string{"storage-memtable-stop-writes-threshold"},
		EnvVars:  []string{"STORAGE_MEM_TABLE_STOP_WRITES_THRESHOLD", "STORAGE_MEMTABLE_STOP_WRITES_THRESHOLD"},
		Value:    cli.NewIntSlice(storage.DefaultMemTableStopWritesThreshold),
		Usage:    `MemTableStopWritesThreshold of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageMaxConcurrentCompaction = &cli.IntSliceFlag{
		Name:     "storage-max-concurrent-compaction",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_MAX_CONCURRENT_COMPACTION"},
		Value:    cli.NewIntSlice(storage.DefaultMaxConcurrentCompactions),
		Usage:    `MaxConcurrentCompaction of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageMetricsLogInterval = &cli.DurationFlag{
		Name:     "storage-metrics-log-interval",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_METRICS_LOG_INTERVAL"},
		Value:    storage.DefaultMetricsLogInterval,
	}
	flagStorageTrimDelay = &cli.DurationFlag{
		Name:     "storage-trim-delay",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_TRIM_DELAY"},
		Usage:    "Delay before deletion of log entries caused by Trim operation. If zero, lazy deletion waits for other log entries to be appended.",
	}
	flagStorageTrimRate = &cli.StringFlag{
		Name:     "storage-trim-rate",
		Category: categoryStorage,
		EnvVars:  []string{"STORAGE_TRIM_RATE"},
		Usage:    "Trim deletion throttling rate in bytes per second. If zero, no throttling is applied.",
	}
)
