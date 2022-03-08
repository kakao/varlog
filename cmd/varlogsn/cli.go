package main

import (
	"time"

	"github.com/urfave/cli/v2"

	"github.daumkakao.com/varlog/varlog/internal/storage"
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/logstream"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/units"
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
			flagServerReadBufferSize.StringFlag(false, units.ToByteSizeString(storagenode.DefaultServerReadBufferSize)),
			flagServerWriteBufferSize.StringFlag(false, units.ToByteSizeString(storagenode.DefaultServerWriteBufferSize)),
			flagServerMaxRecvMsgSize.StringFlag(false, units.ToByteSizeString(storagenode.DefaultServerMaxRecvSize)),
			flagReplicationClientReadBufferSize.StringFlag(false, units.ToByteSizeString(storagenode.DefaultReplicateClientReadBufferSize)),
			flagReplicationClientWriteBufferSize.StringFlag(false, units.ToByteSizeString(storagenode.DefaultReplicateClientWriteBufferSize)),

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
