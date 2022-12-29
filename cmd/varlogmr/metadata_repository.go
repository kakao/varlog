package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/urfave/cli/v2"
	_ "go.uber.org/automaxprocs"

	"github.com/kakao/varlog/internal/metarepos"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/log"
	"github.com/kakao/varlog/pkg/util/units"
)

func main() {
	app := initCLI()
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}

func start(c *cli.Context) error {
	logDir, err := filepath.Abs(c.String(flagLogDir.Name))
	if err != nil {
		return fmt.Errorf("could not create abs path: %w", err)
	}
	logger, err := log.New(
		log.WithoutLogToStderr(),
		log.WithPath(fmt.Sprintf("%s/log.txt", logDir)),
	)
	if err != nil {
		return fmt.Errorf("could not create logger: %w", err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	cid, err := types.ParseClusterID(c.String(flagClusterID.Name))
	if err != nil {
		return fmt.Errorf("cluster id: %w", err)
	}

	readBufferSize, err := units.FromByteSizeString(c.String(flagReportCommitterReadBufferSize.Name))
	if err != nil {
		return err
	}

	writeBufferSize, err := units.FromByteSizeString(c.String(flagReportCommitterWriteBufferSize.Name))
	if err != nil {
		return err
	}

	opts := []metarepos.Option{
		metarepos.WithClusterID(cid),
		metarepos.WithRPCAddress(c.String(flagRPCAddr.Name)),
		metarepos.WithRaftAddress(c.String(flagRaftAddr.Name)),
		metarepos.WithDebugAddress(c.String(flagDebugAddr.Name)),
		metarepos.WithRaftDirectory(c.String(flagRaftDir.Name)),
		metarepos.WithReplicationFactor(c.Int(flagReplicationFactor.Name)),
		metarepos.WithSnapshotCount(c.Uint64(flagSnapshotCount.Name)),
		metarepos.WithMaxSnapPurgeCount(c.Uint(flagMaxSnapPurgeCount.Name)),
		metarepos.WithMaxWALPurgeCount(c.Uint(flagMaxWALPurgeCount.Name)),
		metarepos.WithReportCommitterReadBufferSize(int(readBufferSize)),
		metarepos.WithReportCommitterWriteBufferSize(int(writeBufferSize)),
		metarepos.WithPeers(c.StringSlice(flagPeers.Name)...),
		metarepos.WithMaxTopicsCount(int32(c.Int(flagMaxTopicsCount.Name))),
		metarepos.WithMaxLogStreamsCountPerTopic(int32(c.Int(flagMaxLogStreamsCountPerTopic.Name))),
		metarepos.WithTelemetryCollectorName(c.String(flagTelemetryCollectorName.Name)),
		metarepos.WithTelemetryCollectorEndpoint(c.String(flagTelemetryCollectorEndpoint.Name)),
		metarepos.WithLogger(logger),
	}
	if c.Bool(flagJoin.Name) {
		opts = append(opts, metarepos.JoinCluster())
	}

	mr := metarepos.NewRaftMetadataRepository(opts...)
	mr.Run()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigC
		mr.Close()
	}()

	mr.Wait()
	return nil
}

func initCLI() *cli.App {
	return &cli.App{
		Name:    "metadata_repository",
		Usage:   "run metadata repository",
		Version: "v0.0.1",
		Commands: []*cli.Command{{
			Name:    "start",
			Aliases: []string{"s"},
			Usage:   "start [flags]",
			Action:  start,
			Flags: []cli.Flag{
				flagClusterID.StringFlag(false, metarepos.DefaultClusterID.String()),
				flagRPCAddr.StringFlag(false, metarepos.DefaultRPCBindAddress),
				flagRaftAddr.StringFlag(false, metarepos.DefaultRaftAddress),
				flagDebugAddr.StringFlag(false, metarepos.DefaultDebugAddress),
				flagReplicationFactor.IntFlag(false, metarepos.DefaultLogReplicationFactor),
				flagRaftProposeTimeout.DurationFlag(false, metarepos.DefaultProposeTimeout),
				flagRPCTimeout.DurationFlag(false, metarepos.DefaultRPCTimeout),
				flagCommitTick.DurationFlag(false, metarepos.DefaultCommitTick),
				flagPromoteTick.DurationFlag(false, metarepos.DefaultPromoteTick),
				flagJoin.BoolFlag(),
				flagSnapshotCount.Uint64Flag(false, metarepos.DefaultSnapshotCount),
				flagMaxSnapshotCatchUpCount.Uint64Flag(false, metarepos.DefaultSnapshotCatchUpCount),
				flagMaxSnapPurgeCount.UintFlag(false, metarepos.DefaultSnapshotPurgeCount),
				flagMaxWALPurgeCount.UintFlag(false, metarepos.DefaultWalPurgeCount),
				flagRaftTick.DurationFlag(false, metarepos.DefaultRaftTick),
				flagRaftDir.StringFlag(false, metarepos.DefaultRaftDir),
				flagPeers.StringSliceFlag(false, nil),
				flagReportCommitterReadBufferSize.StringFlag(false, units.ToByteSizeString(metarepos.DefaultReportCommitterReadBufferSize)),
				flagReportCommitterWriteBufferSize.StringFlag(false, units.ToByteSizeString(metarepos.DefaultReportCommitterWriteBufferSize)),
				flagMaxTopicsCount,
				flagMaxLogStreamsCountPerTopic,
				flagTelemetryCollectorName.StringFlag(false, metarepos.DefaultTelemetryCollectorName),
				flagTelemetryCollectorEndpoint.StringFlag(false, metarepos.DefaultTelmetryCollectorEndpoint),
				flagLogDir.StringFlag(false, metarepos.DefaultLogDir),
			},
		}},
	}
}
