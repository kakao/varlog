package main

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/pkg/util/units"
)

var (
	flagClusterID     = flags.ClusterID()
	flagStorageNodeID = flags.StorageNodeID()
	flagListen        = flags.FlagDesc{
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

	// flags for grpc options.
	flagServerReadBufferSize = flags.FlagDesc{
		Name:  "server-read-buffer-size",
		Envs:  []string{"SERVER_READ_BUFFER_SIZE"},
		Usage: "B, KiB, MiB, GiB",
	}
	flagServerWriteBufferSize = flags.FlagDesc{
		Name:  "server-write-buffer-size",
		Envs:  []string{"SERVER_WRITE_BUFFER_SIZE"},
		Usage: "B, KiB, MiB, GiB",
	}
	flagServerMaxRecvMsgSize = flags.FlagDesc{
		Name:    "server-max-msg-size",
		Aliases: []string{"server-max-message-size"},
		Envs:    []string{"SERVER_MAX_MSG_SIZE", "SERVER_MAX_MESSAGE_SIZE"},
		Usage:   "B, KiB, MiB, GiB",
	}
	flagServerInitialConnWindowSize = &cli.StringFlag{
		Name:    "server-initial-conn-window-size",
		EnvVars: []string{"SERVER_INITIAL_CONN_WINDOW_SIZE"},
		Usage:   "Window size for a connection.",
	}
	flagServerInitialStreamWindowSize = &cli.StringFlag{
		Name:    "server-initial-stream-window-size",
		EnvVars: []string{"SERVER_INITIAL_STREAM_WINDOW_SIZE"},
		Usage:   "Window size for stream.",
	}
	flagReplicationClientReadBufferSize = flags.FlagDesc{
		Name:  "replication-client-read-buffer-size",
		Envs:  []string{"REPLICATION_CLIENT_READ_BUFFER_SIZE"},
		Usage: "B, KiB, MiB, GiB",
	}
	flagReplicationClientWriteBufferSize = flags.FlagDesc{
		Name:  "replication-client-write-buffer-size",
		Envs:  []string{"REPLICATION_CLIENT_WRITE_BUFFER_SIZE"},
		Usage: "B, KiB, MiB, GiB",
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
	flagExperimentalStorageSeparateDB = &cli.BoolFlag{
		Name:    "experimental-storage-separate-db",
		EnvVars: []string{"EXPERIMENTAL_STORAGE_SEPARATE_DB"},
		Usage:   "Separate databases of storage experimentally.",
	}
	flagStorageDisableWAL = flags.FlagDesc{
		Name: "storage-disable-wal",
		Envs: []string{"STORAGE_DISABLE_WAL"},
	}
	flagStorageNoSync = flags.FlagDesc{
		Name: "storage-no-sync",
		Envs: []string{"STORAGE_NO_SYNC"},
	}
	flagStorageVerbose = flags.FlagDesc{
		Name: "storage-verbose",
		Envs: []string{"STORAGE_VERBOSE"},
	}

	flagStorageL0CompactionFileThreshold = &cli.IntSliceFlag{
		Name:    "storage-l0-compaction-file-threshold",
		EnvVars: []string{"STORAGE_L0_COMPACTION_FILE_THRESHOLD"},
		Value:   cli.NewIntSlice(storage.DefaultL0CompactionFileThreshold),
		Usage:   `L0CompactionFileThreshold of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageL0CompactionThreshold = &cli.IntSliceFlag{
		Name:    "storage-l0-compaction-threshold",
		EnvVars: []string{"STORAGE_L0_COMPACTION_THRESHOLD"},
		Value:   cli.NewIntSlice(storage.DefaultL0CompactionThreshold),
		Usage:   `L0CompactionThreshold of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageL0StopWritesThreshold = &cli.IntSliceFlag{
		Name:    "storage-l0-stop-writes-threshold",
		EnvVars: []string{"STORAGE_L0_STOP_WRITES_THRESHOLD"},
		Value:   cli.NewIntSlice(storage.DefaultL0StopWritesThreshold),
		Usage:   `L0StopWritesThreshold of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageL0TargetFileSize = &cli.StringSliceFlag{
		Name:    "storage-l0-target-file-size",
		EnvVars: []string{"STORAGE_L0_TARGET_FILE_SIZE"},
		Value:   cli.NewStringSlice(units.ToByteSizeString(storage.DefaultL0TargetFileSize)),
		Usage:   `L0TargetFileSize of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageFlushSplitBytes = &cli.StringSliceFlag{
		Name:    "storage-flush-split-bytes",
		EnvVars: []string{"STORAGE_FLUSH_SPLIT_BYTES"},
		Value:   cli.NewStringSlice(units.ToByteSizeString(storage.DefaultFlushSplitBytes)),
		Usage:   `FlushSplitBytes of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageLBaseMaxBytes = &cli.StringSliceFlag{
		Name:    "storage-lbase-max-bytes",
		EnvVars: []string{"STORAGE_LBASE_MAX_BYTES"},
		Value:   cli.NewStringSlice(units.ToByteSizeString(storage.DefaultLBaseMaxBytes)),
		Usage:   `LBaseMaxBytes of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageMaxOpenFiles = &cli.IntSliceFlag{
		Name:    "storage-max-open-files",
		EnvVars: []string{"STORAGE_MAX_OPEN_FILES"},
		Value:   cli.NewIntSlice(storage.DefaultMaxOpenFiles),
		Usage:   `MaxOpenFiles of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageMemTableSize = &cli.StringSliceFlag{
		Name:    "storage-mem-table-size",
		Aliases: []string{"storage-memtable-size"},
		EnvVars: []string{"STORAGE_MEM_TABLE_SIZE", "STORAGE_MEMTABLE_SIZE"},
		Value:   cli.NewStringSlice(units.ToByteSizeString(storage.DefaultMemTableSize)),
		Usage:   `MemTableSize of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageMemTableStopWritesThreshold = &cli.IntSliceFlag{
		Name:    "storage-mem-table-stop-writes-threshold",
		Aliases: []string{"storage-memtable-stop-writes-threshold"},
		EnvVars: []string{"STORAGE_MEM_TABLE_STOP_WRITES_THRESHOLD", "STORAGE_MEMTABLE_STOP_WRITES_THRESHOLD"},
		Value:   cli.NewIntSlice(storage.DefaultMemTableStopWritesThreshold),
		Usage:   `MemTableStopWritesThreshold of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageMaxConcurrentCompaction = &cli.IntSliceFlag{
		Name:    "storage-max-concurrent-compaction",
		EnvVars: []string{"STORAGE_MAX_CONCURRENT_COMPACTION"},
		Value:   cli.NewIntSlice(storage.DefaultMaxConcurrentCompactions),
		Usage:   `MaxConcurrentCompaction of storage database. Comma-separated values are allowed if the "--experimental-storage-separate-db" is used, for example, <value for data DB>,<value for commit DB>.`,
	}
	flagStorageMetricsLogInterval = &cli.DurationFlag{
		Name:    "storage-metrics-log-interval",
		EnvVars: []string{"STORAGE_METRICS_LOG_INTERVAL"},
		Value:   storage.DefaultMetricsLogInterval,
	}
)
