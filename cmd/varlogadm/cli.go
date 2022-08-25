package main

import (
	"context"
	"path/filepath"

	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/kakao/varlog/internal/admin"
	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/internal/admin/snmanager"
	"github.com/kakao/varlog/internal/admin/snwatcher"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/log"
)

func newAdminApp() *cli.App {
	return &cli.App{
		Name:    "varlogadm",
		Usage:   "run varlog admin server",
		Version: "0.0.1",
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
			flagClusterID.StringFlag(false, types.ClusterID(1).String()),
			flagListen.StringFlag(false, admin.DefaultListenAddress),
			flagReplicationFactor.UintFlag(false, admin.DefaultReplicationFactor),
			flagLogStreamGCTimeout.DurationFlag(false, admin.DefaultLogStreamGCTimeout),
			flagDisableAutoLogStreamSync.BoolFlag(),
			flagAutoUnseal.BoolFlag(),

			flagMetadataRepository.StringSliceFlag(true, nil),
			flagInitMRConnRetryCount.IntFlag(false, mrmanager.DefaultInitialMRConnectRetryCount),
			flagInitMRConnRetryBackoff.DurationFlag(false, mrmanager.DefaultInitialMRConnectRetryBackoff),
			flagMRConnTimeout.DurationFlag(false, mrmanager.DefaultMRConnTimeout),
			flagMRCallTimeout.DurationFlag(false, mrmanager.DefaultMRCallTimeout),

			flagSNWatcherHeartbeatCheckDeadline.DurationFlag(false, snwatcher.DefaultHeartbeatDeadline),
			flagSNWatcherReportDeadline.DurationFlag(false, snwatcher.DefaultReportDeadline),

			flagLogDir.StringFlag(false, ""),
			flagLogToStderr.BoolFlag(),
			flagLogFileRetentionDays.IntFlag(false, 0),
			flagLogFileCompression.BoolFlag(),
			flagLogLevel.StringFlag(false, "info"),
		},
	}
}

func start(c *cli.Context) error {
	clusterID, err := types.ParseClusterID(c.String(flagClusterID.Name))
	if err != nil {
		return err
	}
	logger, err := newLogger(c)
	if err != nil {
		return err
	}
	logger = logger.Named("adm").With(zap.Uint32("cid", uint32(clusterID)))
	defer func() {
		_ = logger.Sync()
	}()

	mrMgr, err := mrmanager.New(context.TODO(),
		mrmanager.WithAddresses(c.StringSlice(flagMetadataRepository.Name)...),
		mrmanager.WithInitialMRConnRetryCount(c.Int(flagInitMRConnRetryCount.Name)),
		mrmanager.WithInitialMRConnRetryBackoff(c.Duration(flagInitMRConnRetryBackoff.Name)),
		mrmanager.WithMRManagerConnTimeout(c.Duration(flagMRConnTimeout.Name)),
		mrmanager.WithMRManagerCallTimeout(c.Duration(flagMRCallTimeout.Name)),
		mrmanager.WithClusterID(clusterID),
		mrmanager.WithLogger(logger),
	)
	if err != nil {
		return err
	}

	snMgr, err := snmanager.New(context.TODO(),
		snmanager.WithClusterID(clusterID),
		snmanager.WithClusterMetadataView(mrMgr.ClusterMetadataView()),
		snmanager.WithLogger(logger),
	)
	if err != nil {
		return err
	}

	opts := []admin.Option{
		admin.WithLogger(logger),
		admin.WithListenAddress(c.String(flagListen.Name)),
		admin.WithReplicationFactor(c.Uint(flagReplicationFactor.Name)),
		admin.WithLogStreamGCTimeout(c.Duration(flagLogStreamGCTimeout.Name)),
		admin.WithMetadataRepositoryManager(mrMgr),
		admin.WithStorageNodeManager(snMgr),
		admin.WithStorageNodeWatcherOptions(
			snwatcher.WithHeartbeatCheckDeadline(c.Duration(flagSNWatcherHeartbeatCheckDeadline.Name)),
			snwatcher.WithReportDeadline(c.Duration(flagSNWatcherReportDeadline.Name)),
		),
	}
	if c.Bool(flagDisableAutoLogStreamSync.Name) {
		opts = append(opts, admin.WithoutAutoLogStreamSync())
	}
	if c.Bool(flagAutoUnseal.Name) {
		opts = append(opts, admin.WithAutoUnseal())
	}
	return Main(opts, logger)
}

func newLogger(c *cli.Context) (*zap.Logger, error) {
	level, err := zapcore.ParseLevel(c.String(flagLogLevel.Name))
	if err != nil {
		return nil, err
	}

	opts := []log.Option{
		log.WithHumanFriendly(),
		log.WithLocalTime(),
		log.WithZapLoggerOptions(zap.AddStacktrace(zap.DPanicLevel)),
		log.WithLogLevel(level),
	}
	if !c.Bool(flagLogToStderr.Name) {
		opts = append(opts, log.WithoutLogToStderr())
	}
	if logdir := c.String(flagLogDir.Name); len(logdir) != 0 {
		absDir, err := filepath.Abs(logdir)
		if err != nil {
			return nil, err
		}
		opts = append(opts, log.WithPath(filepath.Join(absDir, "varlogadm.log")))
	}
	if c.Bool(flagLogFileCompression.Name) {
		opts = append(opts, log.WithCompression())
	}
	if retention := c.Int(flagLogFileRetentionDays.Name); retention > 0 {
		opts = append(opts, log.WithAgeDays(retention))
	}
	return log.New(opts...)
}
