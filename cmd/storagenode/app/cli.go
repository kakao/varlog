package app

import (
	"github.com/urfave/cli/v2"

	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/executor"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/storage"
	"github.daumkakao.com/varlog/varlog/pkg/util/units"
	"github.daumkakao.com/varlog/varlog/pkg/vflag"
)

func InitCLI() *cli.App {
	app := &cli.App{
		Name:    "storagenode",
		Usage:   "run storage node",
		Version: "v0.0.1",
	}

	cli.VersionFlag = &cli.BoolFlag{Name: "version"}

	app.Commands = append(app.Commands, initStartCommand())
	return app
}

func initStartCommand() *cli.Command {
	startCmd := &cli.Command{
		Name:    "start",
		Aliases: []string{"s"},
		Usage:   "start [flags]",
		Action:  Main,
	}

	startCmd.Flags = []cli.Flag{
		// flags for storage node
		flagClusterID.StringFlagV(vflag.DefaultClusterID.String()),
		flagStorageNodeID.StringFlagV(vflag.DefaultStorageNodeID.String()),
		flagVolumes.StringSliceFlag(),
		flagListenAddress.StringFlag(),
		flagAdvertiseAddress.StringFlag(),
		flagBallastSize.StringFlagV(units.ToByteSizeString(storagenode.DefaultBallastSize)),
		flagServerReadBufferSize.StringFlagV(units.ToByteSizeString(storagenode.DefaultServerReadBufferSize)),
		flagServerWriteBufferSize.StringFlagV(units.ToByteSizeString(storagenode.DefaultServerWriteBufferSize)),
		flagReplicationClientReadBufferSize.StringFlagV(units.ToByteSizeString(storagenode.DefaultReplicationClientReadBufferSize)),
		flagReplicationClientWriteBufferSize.StringFlagV(units.ToByteSizeString(storagenode.DefaultReplicationClientWriteBufferSize)),

		// flags for logging
		flagLogDir.StringFlag(),
		flagLogToStderr.BoolFlag(),

		// flags for executor
		flagWriteQueueSize.IntFlagV(executor.DefaultWriteQueueSize),
		flagWriteBatchSize.IntFlagV(executor.DefaultWriteBatchSize),
		flagCommitQueueSize.IntFlagV(executor.DefaultCommitQueueSize),
		flagCommitBatchSize.IntFlagV(executor.DefaultCommitBatchSize),
		flagReplicateQueueSize.IntFlagV(executor.DefaultReplicateQueueSize),

		// flags for storage
		flagDisableWriteSync.BoolFlag(),
		flagDisableCommitSync.BoolFlag(),
		flagDisableDeleteCommittedSync.BoolFlag(),
		flagDisableDeleteUncommittedSync.BoolFlag(),
		flagMemTableSizeBytes.IntFlagV(storage.DefaultMemTableSize),
		flagMemTableStopWritesThreshold.IntFlagV(storage.DefaultMemTableStopWritesThreshold),
		flagStorageDebugLog.BoolFlag(),

		// flags for telemetry
		flagExporterType.StringFlag(),
		flagExporterStopTimeout.DurationFlag(),
		flagStdoutExporterPrettyPrint.BoolFlag(),
		flagOTLPExporterInsecure.BoolFlag(),
		flagOTLPExporterEndpoint.StringFlag(),
	}
	return startCmd
}
