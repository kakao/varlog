package app

import (
	"github.com/urfave/cli/v2"
	"github.daumkakao.com/varlog/varlog/internal/storage"
)

func InitCLI(options *storage.StorageNodeOptions) *cli.App {
	app := &cli.App{
		Name:    "storagenode",
		Usage:   "run storage node",
		Version: "v0.0.1",
	}

	cli.VersionFlag = &cli.BoolFlag{Name: "version"}
	app.Commands = append(app.Commands, initStartCommand(options))
	return app
}

func initStartCommand(options *storage.StorageNodeOptions) *cli.Command {
	startCmd := &cli.Command{
		Name:    "start",
		Aliases: []string{"s"},
		Usage:   "start [flags]",
		Action: func(c *cli.Context) error {
			return Main(options)
		},
	}

	startCmd.Flags = []cli.Flag{
		&cli.BoolFlag{
			Name:        "verbose",
			Aliases:     []string{"v"},
			Value:       false,
			Usage:       "verbose output",
			EnvVars:     []string{"VERBOSE"},
			Destination: &options.Verbose,
		},
	}
	startCmd.Flags = append(startCmd.Flags, initRPCFlags(&options.RPCOptions)...)
	startCmd.Flags = append(startCmd.Flags, initLSEFlags(&options.LogStreamExecutorOptions)...)
	return startCmd
}

func initRPCFlags(options *storage.RPCOptions) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "rpc-bind-address",
			Aliases:     []string{},
			Value:       storage.DefaultRPCBindAddress,
			Usage:       "RPC bind address",
			EnvVars:     []string{"RPC_BIND_ADDRESS"},
			Destination: &options.RPCBindAddress,
		},
	}
}

func initLSEFlags(options *storage.LogStreamExecutorOptions) []cli.Flag {
	return []cli.Flag{
		&cli.UintFlag{
			Name:        "lse-appendc-size",
			Aliases:     []string{},
			Value:       storage.DefaultLSEAppendCSize,
			Usage:       "Size of append channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_APPENDC_SIZE"},
			Destination: &options.AppendCSize,
		},
		&cli.DurationFlag{
			Name:        "lse-appendc-timeout",
			Aliases:     []string{},
			Value:       storage.DefaultLSEAppendCTimeout,
			Usage:       "Timeout for append channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_APPENDC_TIMEOUT"},
			DefaultText: "infinity",
			Destination: &options.AppendCTimeout,
		},
		&cli.UintFlag{
			Name:        "lse-commitc-size",
			Aliases:     []string{},
			Value:       storage.DefaultLSECommitCSize,
			Usage:       "Size of commit channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_COMMITC_SIZE"},
			Destination: &options.CommitCSize,
		},
		&cli.DurationFlag{
			Name:        "lse-commitc-timeout",
			Aliases:     []string{},
			Value:       storage.DefaultLSECommitCTimeout,
			Usage:       "Timeout for commit channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_COMMITC_TIMEOUT"},
			Destination: &options.CommitCTimeout,
		},
		&cli.UintFlag{
			Name:        "lse-trimc-size",
			Aliases:     []string{},
			Value:       storage.DefaultLSETrimCSize,
			Usage:       "Size of trim channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_TRIMC_SIZE"},
			Destination: &options.TrimCSize,
		},
		&cli.DurationFlag{
			Name:        "lse-trimc-timeout",
			Aliases:     []string{},
			Value:       storage.DefaultLSETrimCTimeout,
			Usage:       "Timeout for trim channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_TRIMC_TIMEOUT"},
			Destination: &options.TrimCTimeout,
		},
	}
}
