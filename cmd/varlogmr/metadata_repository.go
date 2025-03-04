package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v2"
	_ "go.uber.org/automaxprocs"

	"github.com/kakao/varlog/internal/buildinfo"
	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/internal/metarepos"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/log"
	"github.com/kakao/varlog/pkg/util/telemetry"
)

func main() {
	app := initCLI()
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}

func start(c *cli.Context) error {
	logOpts, err := flags.ParseLoggerFlags(c, "varlogmr.log")
	if err != nil {
		return err
	}
	logger, err := log.New(logOpts...)
	if err != nil {
		return err
	}
	defer func() {
		_ = logger.Sync()
	}()

	cid, err := types.ParseClusterID(c.String(flagClusterID.Name))
	if err != nil {
		return fmt.Errorf("cluster id: %w", err)
	}

	grpcDialOpts, err := flags.ParseGRPCDialOptionFlags(c)
	if err != nil {
		return err
	}

	raftAddr := c.String(flagRaftAddr.Name)
	nodeID := types.NewNodeIDFromURL(raftAddr)
	meterProviderOpts, err := flags.ParseTelemetryFlags(context.Background(), c, "mr", nodeID.String(), cid)
	if err != nil {
		return err
	}
	mp, stop, err := telemetry.NewMeterProvider(meterProviderOpts...)
	if err != nil {
		return err
	}
	telemetry.SetGlobalMeterProvider(mp)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.Duration(flags.TelemetryExporterStopTimeout.Name))
		defer cancel()
		_ = stop(ctx)
	}()

	opts := []metarepos.Option{
		metarepos.WithClusterID(cid),
		metarepos.WithRPCAddress(c.String(flagRPCAddr.Name)),
		metarepos.WithRaftAddress(raftAddr),
		metarepos.WithDebugAddress(c.String(flagDebugAddr.Name)),
		metarepos.WithRaftDirectory(c.String(flagRaftDir.Name)),
		metarepos.WithReplicationFactor(c.Int(flagReplicationFactor.Name)),
		metarepos.WithSnapshotCount(c.Uint64(flagSnapshotCount.Name)),
		metarepos.WithMaxSnapPurgeCount(c.Uint(flagMaxSnapPurgeCount.Name)),
		metarepos.WithMaxWALPurgeCount(c.Uint(flagMaxWALPurgeCount.Name)),
		metarepos.WithDefaultGRPCDialOptions(grpcDialOpts...),
		metarepos.WithPeers(c.StringSlice(flagPeers.Name)...),
		metarepos.WithMaxTopicsCount(int32(c.Int(flagMaxTopicsCount.Name))),
		metarepos.WithMaxLogStreamsCountPerTopic(int32(c.Int(flagMaxLogStreamsCountPerTopic.Name))),
		metarepos.WithTelemetryCollectorName(c.String(flagTelemetryCollectorName.Name)),
		metarepos.WithTelemetryCollectorEndpoint(c.String(flagTelemetryCollectorEndpoint.Name)),
		metarepos.WithCommitTick(c.Duration(flagCommitTick.Name)),
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
	buildInfo := buildinfo.ReadVersionInfo()
	cli.VersionPrinter = func(*cli.Context) {
		fmt.Println(buildInfo.String())
	}
	return &cli.App{
		Name:    "metadata_repository",
		Usage:   "run metadata repository",
		Version: buildInfo.Version,
		Commands: []*cli.Command{{
			Name:    "start",
			Aliases: []string{"s"},
			Usage:   "start [flags]",
			Action:  start,
			Flags: []cli.Flag{
				flagClusterID,
				flagRPCAddr.StringFlag(false, metarepos.DefaultRPCBindAddress),
				flagRaftAddr.StringFlag(false, metarepos.DefaultRaftAddress),
				flagDebugAddr.StringFlag(false, metarepos.DefaultDebugAddress),
				flagReplicationFactor,
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
				flags.GRPCServerReadBufferSize,
				flags.GRPCServerRecvBufferPool,
				flags.GRPCServerWriteBufferSize,
				flags.GRPCServerSharedWriteBuffer,
				flags.GRPCServerMaxRecvMsgSize,
				flags.GRPCServerInitialConnWindowSize,
				flags.GRPCServerInitialWindowSize,
				flags.GRPCClientReadBufferSize,
				flags.GRPCClientRecvBufferPool,
				flags.GRPCClientWriteBufferSize,
				flags.GRPCClientSharedWriteBuffer,
				flags.GRPCClientInitialConnWindowSize,
				flags.GRPCClientInitialWindowSize,
				flagMaxTopicsCount,
				flagMaxLogStreamsCountPerTopic,

				// telemetry
				flags.TelemetryExporter,
				flags.TelemetryExporterStopTimeout,
				flags.TelemetryOTLPEndpoint,
				flags.TelemetryOTLPInsecure,
				flags.TelemetryHost,
				flags.TelemetryRuntime,

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
				flags.EnableDevelopmentMode,
			},
		}},
	}
}
