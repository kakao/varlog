package main

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/admin"
	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/internal/admin/snmanager"
	"github.com/kakao/varlog/internal/admin/snwatcher"
	"github.com/kakao/varlog/internal/buildinfo"
	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/internal/stats/opentelemetry"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/log"
)

func newAdminApp() *cli.App {
	buildInfo := buildinfo.ReadVersionInfo()
	cli.VersionPrinter = func(*cli.Context) {
		fmt.Println(buildInfo.String())
	}
	return &cli.App{
		Name:    "varlogadm",
		Usage:   "run varlog admin server",
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
		Usage:   "start [flags]",
		Action:  start,
		Flags: []cli.Flag{
			flagClusterID,
			flagListen.StringFlag(false, admin.DefaultListenAddress),
			flagReplicationFactor,
			flagLogStreamGCTimeout.DurationFlag(false, admin.DefaultLogStreamGCTimeout),
			flagDisableAutoLogStreamSync.BoolFlag(),
			flagAutoUnseal.BoolFlag(),
			flagReplicaSelector,

			flagMetadataRepository.StringSliceFlag(true, nil),
			flagInitMRConnRetryCount.IntFlag(false, mrmanager.DefaultInitialMRConnectRetryCount),
			flagInitMRConnRetryBackoff.DurationFlag(false, mrmanager.DefaultInitialMRConnectRetryBackoff),
			flagMRConnTimeout.DurationFlag(false, mrmanager.DefaultMRConnTimeout),
			flagMRCallTimeout.DurationFlag(false, mrmanager.DefaultMRCallTimeout),

			flagSNWatcherHeartbeatCheckDeadline.DurationFlag(false, snwatcher.DefaultHeartbeatCheckDeadline),
			flagSNWatcherHeartbeatTimeout.DurationFlag(false, snwatcher.DefaultHeartbeatTimeout),
			flagSNWatcherReportDeadline.DurationFlag(false, snwatcher.DefaultReportDeadline),

			flags.GRPCServerReadBufferSize,
			flags.GRPCServerWriteBufferSize,
			flags.GRPCServerSharedWriteBuffer,
			flags.GRPCServerInitialConnWindowSize,
			flags.GRPCServerInitialWindowSize,
			flags.GRPCClientReadBufferSize,
			flags.GRPCClientWriteBufferSize,
			flags.GRPCClientSharedWriteBuffer,
			flags.GRPCClientInitialConnWindowSize,
			flags.GRPCClientInitialWindowSize,

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

func start(c *cli.Context) error {
	clusterID, err := types.ParseClusterID(c.String(flagClusterID.Name))
	if err != nil {
		return err
	}

	logOpts, err := flags.ParseLoggerFlags(c, "varlogadm.log")
	if err != nil {
		return err
	}
	logger, err := log.New(logOpts...)
	if err != nil {
		return err
	}
	logger = logger.Named("adm").With(zap.Int32("cid", int32(clusterID)))
	defer func() {
		_ = logger.Sync()
	}()

	meterProviderOpts, err := flags.ParseTelemetryFlags(context.Background(), c, "adm", clusterID.String(), clusterID)
	if err != nil {
		return err
	}
	mp, stop, err := opentelemetry.NewMeterProvider(meterProviderOpts...)
	if err != nil {
		return err
	}
	opentelemetry.SetGlobalMeterProvider(mp)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.Duration(flags.TelemetryExporterStopTimeout.Name))
		defer cancel()
		_ = stop(ctx)
	}()

	repfactor := c.Int(flagReplicationFactor.Name)

	grpcDialOpts, err := flags.ParseGRPCDialOptionFlags(c)
	if err != nil {
		return err
	}

	mrMgr, err := mrmanager.New(context.TODO(),
		mrmanager.WithAddresses(c.StringSlice(flagMetadataRepository.Name)...),
		mrmanager.WithInitialMRConnRetryCount(c.Int(flagInitMRConnRetryCount.Name)),
		mrmanager.WithInitialMRConnRetryBackoff(c.Duration(flagInitMRConnRetryBackoff.Name)),
		mrmanager.WithMRManagerConnTimeout(c.Duration(flagMRConnTimeout.Name)),
		mrmanager.WithMRManagerCallTimeout(c.Duration(flagMRCallTimeout.Name)),
		mrmanager.WithClusterID(clusterID),
		mrmanager.WithReplicationFactor(repfactor),
		mrmanager.WithLogger(logger),
		mrmanager.WithDefaultGRPCDialOptions(grpcDialOpts...),
	)
	if err != nil {
		return err
	}

	snMgr, err := snmanager.New(context.TODO(),
		snmanager.WithClusterID(clusterID),
		snmanager.WithClusterMetadataView(mrMgr.ClusterMetadataView()),
		snmanager.WithDefaultGRPCDialOptions(grpcDialOpts...),
		snmanager.WithLogger(logger),
	)
	if err != nil {
		return err
	}

	repsel, err := admin.NewReplicaSelector(c.String(flagReplicaSelector.Name), mrMgr.ClusterMetadataView(), repfactor)
	if err != nil {
		return err
	}

	grpcServerOpts, err := flags.ParseGRPCServerOptionFlags(c)
	if err != nil {
		return err
	}

	opts := []admin.Option{
		admin.WithLogger(logger),
		admin.WithListenAddress(c.String(flagListen.Name)),
		admin.WithReplicationFactor(repfactor),
		admin.WithReplicaSelector(repsel),
		admin.WithLogStreamGCTimeout(c.Duration(flagLogStreamGCTimeout.Name)),
		admin.WithMetadataRepositoryManager(mrMgr),
		admin.WithStorageNodeManager(snMgr),
		admin.WithStorageNodeWatcherOptions(
			snwatcher.WithHeartbeatTimeout(c.Duration(flagSNWatcherHeartbeatTimeout.Name)),
			snwatcher.WithHeartbeatCheckDeadline(c.Duration(flagSNWatcherHeartbeatCheckDeadline.Name)),
			snwatcher.WithReportDeadline(c.Duration(flagSNWatcherReportDeadline.Name)),
		),
		admin.WithDefaultGRPCServerOptions(grpcServerOpts...),
	}
	if c.Bool(flagDisableAutoLogStreamSync.Name) {
		opts = append(opts, admin.WithoutAutoLogStreamSync())
	}
	if c.Bool(flagAutoUnseal.Name) {
		opts = append(opts, admin.WithAutoUnseal())
	}
	return Main(opts, logger)
}
