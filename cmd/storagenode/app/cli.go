package app

import (
	"github.com/urfave/cli/v2"
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

func InitCLI(options *storagenode.Options) *cli.App {
	app := &cli.App{
		Name:    "storagenode",
		Usage:   "run storage node",
		Version: "v0.0.1",
	}

	cli.VersionFlag = &cli.BoolFlag{Name: "version"}
	app.Commands = append(app.Commands, initStartCommand(options))
	return app
}

func initStartCommand(options *storagenode.Options) *cli.Command {
	startCmd := &cli.Command{
		Name:    "start",
		Aliases: []string{"s"},
		Usage:   "start [flags]",
		Action: func(c *cli.Context) error {
			var err error

			// ClusterID
			parsedClusterID := c.Uint("cluster-id")
			if options.ClusterID, err = types.NewClusterIDFromUint(parsedClusterID); err != nil {
				return err
			}

			// StorageNodeID
			if c.IsSet("storage-node-id") {
				parsedStorageNodeID := c.Uint("storage-node-id")
				if options.StorageNodeID, err = types.NewStorageNodeIDFromUint(parsedStorageNodeID); err != nil {
					return err
				}
			} else {
				options.StorageNodeID = types.NewStorageNodeID()
			}

			// Volumes
			options.Volumes = make(map[storagenode.Volume]struct{})
			for _, p := range c.StringSlice("volumes") {
				volume, err := storagenode.NewVolume(p)
				if err != nil {
					return err
				}
				options.Volumes[volume] = struct{}{}
			}

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
	startCmd.Flags = append(startCmd.Flags, initStorageNodeFlags(options)...)
	startCmd.Flags = append(startCmd.Flags, initRPCFlags(&options.RPCOptions)...)
	startCmd.Flags = append(startCmd.Flags, initLSEFlags(&options.LogStreamExecutorOptions)...)
	startCmd.Flags = append(startCmd.Flags, initLSRFlags(&options.LogStreamReporterOptions)...)
	return startCmd
}

func initStorageNodeFlags(options *storagenode.Options) []cli.Flag {
	defaultVolumeSS := cli.NewStringSlice(string(storagenode.DefaultVolume))
	return []cli.Flag{
		&cli.UintFlag{
			Name:    "cluster-id",
			Aliases: []string{},
			Usage:   "cluster id",
			EnvVars: []string{"CLUSTER_ID"},
		},
		&cli.UintFlag{
			Name:    "storage-node-id",
			Aliases: []string{},
			Usage:   "storage node id",
			EnvVars: []string{"STORAGE_NODE_ID"},
		},
		&cli.StringSliceFlag{
			Name:        "volumes",
			Aliases:     []string{},
			Value:       defaultVolumeSS,
			Usage:       "volumes",
			EnvVars:     []string{"VOLUMES"},
			Destination: defaultVolumeSS,
		},
		&cli.StringFlag{
			Name:        "storage-name",
			Aliases:     []string{},
			Value:       storagenode.DefaultStorageName,
			Usage:       "storage name",
			EnvVars:     []string{"STORAGE_NAME"},
			Destination: &options.StorageName,
		},
	}
}

func initRPCFlags(options *storagenode.RPCOptions) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "rpc-bind-address",
			Aliases:     []string{},
			Value:       storagenode.DefaultRPCBindAddress,
			Usage:       "RPC bind address",
			EnvVars:     []string{"RPC_BIND_ADDRESS"},
			Destination: &options.RPCBindAddress,
		},
	}
}

func initLSEFlags(options *storagenode.LogStreamExecutorOptions) []cli.Flag {
	return []cli.Flag{
		&cli.UintFlag{
			Name:        "lse-appendc-size",
			Aliases:     []string{},
			Value:       storagenode.DefaultLSEAppendCSize,
			Usage:       "Size of append channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_APPENDC_SIZE"},
			Destination: &options.AppendCSize,
		},
		&cli.DurationFlag{
			Name:        "lse-appendc-timeout",
			Aliases:     []string{},
			Value:       storagenode.DefaultLSEAppendCTimeout,
			Usage:       "Timeout for append channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_APPENDC_TIMEOUT"},
			DefaultText: "infinity",
			Destination: &options.AppendCTimeout,
		},
		&cli.UintFlag{
			Name:        "lse-commitc-size",
			Aliases:     []string{},
			Value:       storagenode.DefaultLSECommitCSize,
			Usage:       "Size of commit channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_COMMITC_SIZE"},
			Destination: &options.CommitCSize,
		},
		&cli.DurationFlag{
			Name:        "lse-commitc-timeout",
			Aliases:     []string{},
			Value:       storagenode.DefaultLSECommitCTimeout,
			Usage:       "Timeout for commit channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_COMMITC_TIMEOUT"},
			Destination: &options.CommitCTimeout,
		},
		&cli.UintFlag{
			Name:        "lse-trimc-size",
			Aliases:     []string{},
			Value:       storagenode.DefaultLSETrimCSize,
			Usage:       "Size of trim channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_TRIMC_SIZE"},
			Destination: &options.TrimCSize,
		},
		&cli.DurationFlag{
			Name:        "lse-trimc-timeout",
			Aliases:     []string{},
			Value:       storagenode.DefaultLSETrimCTimeout,
			Usage:       "Timeout for trim channel in LogStreamExecutor",
			EnvVars:     []string{"LSE_TRIMC_TIMEOUT"},
			Destination: &options.TrimCTimeout,
		},
	}
}

func initLSRFlags(options *storagenode.LogStreamReporterOptions) []cli.Flag {
	return []cli.Flag{
		&cli.UintFlag{
			Name:        "lsr-commitc-size",
			Aliases:     []string{},
			Value:       storagenode.DefaultLSRCommitCSize,
			Usage:       "Size of commit channel in LogStreamReporter",
			EnvVars:     []string{"LSR_COMMITC_SIZE"},
			Destination: &options.CommitCSize,
		},
		&cli.DurationFlag{
			Name:        "lsr-commitc-timeout",
			Aliases:     []string{},
			Value:       storagenode.DefaultLSRCommitCTimeout,
			Usage:       "Timeout for commit channel in LogStreamReporter",
			EnvVars:     []string{"LSR_COMMITC_TIMEOUT"},
			DefaultText: "infinity",
			Destination: &options.CommitCTimeout,
		},
		&cli.UintFlag{
			Name:        "lsr-reportc-size",
			Aliases:     []string{},
			Value:       storagenode.DefaultLSRReportCSize,
			Usage:       "Size of report channel in LogStreamReporter",
			EnvVars:     []string{"LSR_REPORTC_SIZE"},
			Destination: &options.ReportCSize,
		},
		&cli.DurationFlag{
			Name:        "lsr-reportc-timeout",
			Aliases:     []string{},
			Value:       storagenode.DefaultLSRReportCTimeout,
			Usage:       "Timeout for report channel in LogStreamReporter",
			EnvVars:     []string{"LSR_REPORTC_TIMEOUT"},
			DefaultText: "infinity",
			Destination: &options.ReportCTimeout,
		},
		&cli.DurationFlag{
			Name:        "lsr-report-wait-timeout",
			Aliases:     []string{},
			Value:       storagenode.DefaultLSRReportWaitTimeout,
			Usage:       "Timeout for waiting report LogStreamReporter",
			EnvVars:     []string{"LSR_REPORT_WAIT_TIMEOUT"},
			DefaultText: "infinity",
			Destination: &options.ReportWaitTimeout,
		},
	}
}
