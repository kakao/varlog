package main

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/buildinfo"
	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/pkg/types"
)

const (
	appName = "varlogsn"
)

func newStorageNodeApp() *cli.App {
	buildInfo := buildinfo.ReadVersionInfo()
	cli.VersionPrinter = func(*cli.Context) {
		fmt.Println(buildInfo.String())
	}
	return &cli.App{
		Name:    appName,
		Usage:   "storage node",
		Version: buildInfo.Version,
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
			flagClusterID,
			flagStorageNodeID.StringFlag(false, types.StorageNodeID(1).String()),
			flagListen.StringFlag(false, "127.0.0.1:9091"),
			flagAdvertise.StringFlag(false, ""),
			flagBallastSize.StringFlag(false, storagenode.DefaultBallastSize),

			// volumes
			flagVolumes.StringSliceFlag(true, nil),

			flags.GRPCServerReadBufferSize,
			flags.GRPCServerWriteBufferSize,
			flags.GRPCServerMaxRecvMsgSize,
			flags.GRPCServerInitialConnWindowSize,
			flags.GRPCServerInitialWindowSize,
			flags.GRPCClientReadBufferSize,
			flags.GRPCClientWriteBufferSize,
			flags.GRPCClientInitialConnWindowSize,
			flags.GRPCClientInitialWindowSize,

			// lse options
			flagLogStreamExecutorSequenceQueueCapacity.IntFlag(false, logstream.DefaultSequenceQueueCapacity),
			flagLogStreamExecutorWriteQueueCapacity.IntFlag(false, logstream.DefaultWriteQueueCapacity),
			flagLogStreamExecutorCommitQueueCapacity.IntFlag(false, logstream.DefaultCommitQueueCapacity),
			flagLogStreamExecutorReplicateclientQueueCapacity.IntFlag(false, logstream.DefaultReplicateClientQueueCapacity),
			flagMaxLogStreamReplicasCount,
			flagAppendPipelineSize,

			// storage options
			flagExperimentalStorageSeparateDB,
			flagStorageDisableWAL,
			flagStorageNoSync,
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
			flagStorageVerbose,
			flagStorageTrimDelay,
			flagStorageTrimRate,
			flagStorageCacheSize,

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
