package main

import (
	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/units"
)

const (
	appName = "varlogsn"
	version = "0.0.1"
)

func newStorageNodeApp() *cli.App {
	return &cli.App{
		Name:    appName,
		Usage:   "storage node",
		Version: version,
		Commands: []*cli.Command{
			newStartCommand(),
		},
	}
}

func newStartCommand() *cli.Command {
	return &cli.Command{
		Name:    "start",
		Aliases: []string{"s"},
		Action:  start,
		Flags: []cli.Flag{
			flagClusterID.StringFlag(false, types.ClusterID(1).String()),
			flagStorageNodeID.StringFlag(false, types.StorageNodeID(1).String()),
			flagListen.StringFlag(false, "127.0.0.1:9091"),
			flagAdvertise.StringFlag(false, ""),
			flagBallastSize.StringFlag(false, storagenode.DefaultBallastSize),

			// volumes
			flagVolumes.StringSliceFlag(true, nil),

			flagServerReadBufferSize.StringFlag(false, units.ToByteSizeString(storagenode.DefaultServerReadBufferSize)),
			flagServerWriteBufferSize.StringFlag(false, units.ToByteSizeString(storagenode.DefaultServerWriteBufferSize)),
			flagServerMaxRecvMsgSize.StringFlag(false, units.ToByteSizeString(storagenode.DefaultServerMaxRecvSize)),
			flagReplicationClientReadBufferSize.StringFlag(false, units.ToByteSizeString(storagenode.DefaultReplicateClientReadBufferSize)),
			flagReplicationClientWriteBufferSize.StringFlag(false, units.ToByteSizeString(storagenode.DefaultReplicateClientWriteBufferSize)),
			flagServerInitialConnWindowSize,
			flagServerInitialStreamWindowSize,

			// lse options
			flagLogStreamExecutorSequenceQueueCapacity.IntFlag(false, logstream.DefaultSequenceQueueCapacity),
			flagLogStreamExecutorWriteQueueCapacity.IntFlag(false, logstream.DefaultWriteQueueCapacity),
			flagLogStreamExecutorCommitQueueCapacity.IntFlag(false, logstream.DefaultCommitQueueCapacity),
			flagLogStreamExecutorReplicateclientQueueCapacity.IntFlag(false, logstream.DefaultReplicateClientQueueCapacity),
			flagMaxLogStreamReplicasCount,
			flagAppendPipelineSize,

			// storage options
			flagExperimentalStorageSeparateDB,
			flagStorageDisableWAL.BoolFlag(),
			flagStorageNoSync.BoolFlag(),
			flagStorageL0CompactionFileThreshold,
			flagStorageL0CompactionThreshold,
			flagStorageL0StopWritesThreshold,
			flagStorageL0TargetFileSize,
			flagStorageFlushSplitBytes,
			flagStorageLBaseMaxBytes,
			flagStorageMaxOpenFiles,
			flagStorageMemTableSize,
			flagStorageMemTableStopWritesThreshold,
			flagStorageMaxConcurrentCompaction,
			flagStorageMetricsLogInterval,
			flagStorageVerbose.BoolFlag(),

			// logger options
			flags.LogDir,
			flags.LogToStderr,
			flags.LogFileMaxSizeMB,
			flags.LogFileMaxBackups,
			flags.LogFileRetentionDays,
			flags.LogFileNameUTC,
			flags.LogFileCompression,
			flags.LogHumanReadable,
			flags.LogLevel,

			// telemetry
			flags.TelemetryExporter,
			flags.TelemetryExporterStopTimeout,
			flags.TelemetryOTLPEndpoint,
			flags.TelemetryOTLPInsecure,
			flags.TelemetryHost,
			flags.TelemetryRuntime,
		},
	}
}
