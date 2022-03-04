package main

import (
	"time"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/storage"
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
			flagVolumes.StringSliceFlag(true, nil),
			flagBallastSize.StringFlag(false, "1G"),
			flagServerReadBufferSize.StringFlag(false, "32K"),
			flagServerWriteBufferSize.StringFlag(false, "32K"),
			flagReplicationClientReadBufferSize.StringFlag(false, "32K"),
			flagReplicationClientWriteBufferSize.StringFlag(false, "32K"),

			// lse options
			flagLogStreamExecutorSequenceQueueCapacity.IntFlag(false, logstream.DefaultSequenceQueueCapacity),
			flagLogStreamExecutorWriteQueueCapacity.IntFlag(false, logstream.DefaultWriteQueueCapacity),
			flagLogStreamExecutorCommitQueueCapacity.IntFlag(false, logstream.DefaultCommitQueueCapacity),
			flagLogStreamExecutorReplicateclientQueueCapacity.IntFlag(false, logstream.DefaultReplicateClientQueueCapacity),

			// storage options
			flagStorageDisableWAL.BoolFlag(),
			flagStorageNoSync.BoolFlag(),
			flagStorageL0CompactionThreshold.IntFlag(false, storage.DefaultL0CompactionThreshold),
			flagStorageL0StopWritesThreshold.IntFlag(false, storage.DefaultL0StopWritesThreshold),
			flagStorageLBaseMaxBytes.StringFlag(false, units.ToByteSizeString(storage.DefaultLBaseMaxBytes)),
			flagStorageMaxOpenFiles.IntFlag(false, storage.DefaultMaxOpenFiles),
			flagStorageMemTableSize.StringFlag(false, units.ToByteSizeString(storage.DefaultMemTableSize)),
			flagStorageMemTableStopWritesThreshold.IntFlag(false, storage.DefaultMemTableStopWritesThreshold),
			flagStorageMaxConcurrentCompaction.IntFlag(false, storage.DefaultMaxConcurrentCompactions),
			flagStorageVerbose.BoolFlag(),

			flagLogDir.StringFlag(false, ""),
			flagLogToStderr.BoolFlag(),

			flagExporterType.StringFlag(false, "noop"),
			flagExporterStopTimeout.DurationFlag(false, 5*time.Second),
			flagStdoutExporterPrettyPrint.BoolFlag(),
			flagOTLPExporterInsecure.BoolFlag(),
			flagOTLPExporterEndpoint.StringFlag(false, ""),
		},
	}
}
