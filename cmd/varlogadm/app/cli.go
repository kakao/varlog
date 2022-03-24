package app

import (
	"path/filepath"

	"github.com/urfave/cli/v2"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/varlogadm"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/log"
)

func InitCLI() *cli.App {
	app := &cli.App{
		Name:    "varlogadm",
		Usage:   "run varlog admin server",
		Version: "v0.0.1",
	}
	cli.VersionFlag = &cli.BoolFlag{Name: "version"}
	app.Commands = append(app.Commands, initStartCommand())
	return app
}

func initStartCommand() *cli.Command {
	return &cli.Command{
		Name:    "start",
		Aliases: []string{"s"},
		Usage:   "start [flags]",
		Action:  start,
		Flags: []cli.Flag{
			flagClusterID.StringFlag(false, types.ClusterID(1).String()),
			flagListen.StringFlag(false, varlogadm.DefaultListenAddress),
			flagReplicationFactor.UintFlag(false, varlogadm.DefaultReplicationFactor),
			flagMetadataRepository.StringSliceFlag(true, nil),
			flagInitMRConnRetryCount.IntFlag(false, varlogadm.DefaultInitialMRConnectRetryCount),
			flagInitMRConnRetryBackoff.DurationFlag(false, varlogadm.DefaultInitialMRConnectRetryBackoff),
			flagSNWatcherRPCTimeout.DurationFlag(false, varlogadm.DefaultWatcherRPCTimeout),
			flagMRConnTimeout.DurationFlag(false, varlogadm.DefaultMRConnTimeout),
			flagMRCallTimeout.DurationFlag(false, varlogadm.DefaultMRCallTimeout),
			flagLogDir.StringFlag(false, ""),
			flagLogToStderr.BoolFlag(),
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

	opts := varlogadm.DefaultOptions()
	opts.Logger = logger
	opts.MetadataRepositoryAddresses = c.StringSlice(flagMetadataRepository.Name)
	opts.ListenAddress = c.String(flagListen.Name)
	opts.ReplicationFactor = c.Uint(flagReplicationFactor.Name)
	opts.InitialMRConnRetryCount = c.Int(flagInitMRConnRetryCount.Name)
	opts.InitialMRConnRetryBackoff = c.Duration(flagInitMRConnRetryBackoff.Name)
	opts.WatcherOptions.RPCTimeout = c.Duration(flagSNWatcherRPCTimeout.Name)
	opts.MRManagerOptions.ConnTimeout = c.Duration(flagMRConnTimeout.Name)
	opts.MRManagerOptions.CallTimeout = c.Duration(flagMRCallTimeout.Name)
	return Main(opts)
}

func newLogger(c *cli.Context) (*zap.Logger, error) {
	logtostderr := c.Bool(flagLogToStderr.Name)
	logdir := c.String(flagLogDir.Name)
	if !logtostderr && len(logdir) == 0 {
		return zap.NewNop(), nil
	}

	logOpts := []log.Option{
		log.WithHumanFriendly(),
		log.WithZapLoggerOptions(zap.AddStacktrace(zap.DPanicLevel)),
	}
	if !logtostderr {
		logOpts = append(logOpts, log.WithoutLogToStderr())
	}
	if len(logdir) != 0 {
		absDir, err := filepath.Abs(logdir)
		if err != nil {
			return nil, err
		}
		logOpts = append(logOpts, log.WithPath(filepath.Join(absDir, "varlogadm.log")))
	}
	return log.New(logOpts...)
}
