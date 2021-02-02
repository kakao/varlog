package app

import (
	"github.com/urfave/cli/v2"

	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func InitCLI(options *metadata_repository.MetadataRepositoryOptions) *cli.App {
	app := &cli.App{
		Name:    "metadata_repository",
		Usage:   "run metadata repository",
		Version: "v0.0.1",
	}

	cli.VersionFlag = &cli.BoolFlag{Name: "version"}
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

			options.Peers = c.StringSlice("peers")

			return Main(options)
		},
	}

	startCmd.Flags = []cli.Flag{
		&cli.UintFlag{
			Name:    "cluster-id",
			Aliases: []string{"cid"},
			Value:   uint(storagenode.DefaultClusterID),
			Usage:   "cluster id",
			EnvVars: []string{"CLUSTER_ID"},
		},
		&cli.StringFlag{
			Name:        "bind",
			Aliases:     []string{"b"},
			Value:       metadata_repository.DefaultRPCBindAddress,
			Usage:       "Bind Address",
			EnvVars:     []string{"BIND"},
			Destination: &options.RPCBindAddress,
		},
		&cli.StringFlag{
			Name:        "raft-address",
			Aliases:     []string{},
			Value:       metadata_repository.DefaultRaftAddress,
			Usage:       "Raft Address",
			EnvVars:     []string{"RAFT_ADDRESS"},
			Destination: &options.RaftAddress,
		},
		&cli.StringFlag{
			Name:        "raft-dir",
			Aliases:     []string{},
			Value:       metadata_repository.DefaultRaftDir,
			Usage:       "Raft Dir",
			EnvVars:     []string{"RAFT_DIR"},
			Destination: &options.RaftDir,
		},
		&cli.StringFlag{
			Name:        "log-dir",
			Aliases:     []string{},
			Value:       metadata_repository.DefaultLogDir,
			Usage:       "Log Dir",
			EnvVars:     []string{"LOG_DIR"},
			Destination: &options.LogDir,
		},
		&cli.BoolFlag{
			Name:        "verbose",
			Aliases:     []string{"v"},
			Value:       false,
			Usage:       "verbose output",
			EnvVars:     []string{"VERBOSE"},
			Destination: &options.Verbose,
		},
		&cli.BoolFlag{
			Name:        "join",
			Aliases:     []string{},
			Value:       false,
			Usage:       "join to cluster",
			EnvVars:     []string{"JOIN"},
			Destination: &options.Join,
		},
		&cli.BoolFlag{
			Name:        "nosync",
			Aliases:     []string{},
			Value:       false,
			Usage:       "unsafe nosync",
			EnvVars:     []string{"UNSAFE_NOSYNC"},
			Destination: &options.UnsafeNoSync,
		},
		&cli.IntFlag{
			Name:        "log-rep-factor",
			Aliases:     []string{},
			Value:       metadata_repository.DefaultLogReplicationFactor,
			Usage:       "Replication factor or log stream",
			EnvVars:     []string{"LOG_REP_FACTOR"},
			Destination: &options.NumRep,
		},
		&cli.Uint64Flag{
			Name:        "snapshot-count",
			Aliases:     []string{},
			Value:       metadata_repository.DefaultSnapshotCount,
			Usage:       "Count of entries for Snapshot",
			EnvVars:     []string{"SNAPSHOT_COUNT"},
			Destination: &options.SnapCount,
		},
		&cli.StringSliceFlag{
			Name:    "peers",
			Aliases: []string{"p"},
			Usage:   "Peers of cluster",
			EnvVars: []string{"PEERS"},
		},
		&cli.StringFlag{
			Name:        "debug-addr",
			Aliases:     []string{"d"},
			Value:       metadata_repository.DefaultDebugAddress,
			Usage:       "Debug Address",
			EnvVars:     []string{"DEBUG_ADDRESS"},
			Destination: &options.DebugAddress,
		},
	}

	startCmd.Flags = append(startCmd.Flags, initTelemetryFlags(&options.TelemetryOptions)...)

	app.Commands = append(app.Commands, startCmd)
	return app
}

func initTelemetryFlags(options *metadata_repository.TelemetryOptions) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "collector-name",
			Value:       metadata_repository.DefaultTelemetryCollectorName,
			Usage:       "Collector name",
			EnvVars:     []string{"COLLECTOR_NAME"},
			Destination: &options.CollectorName,
		},
		&cli.StringFlag{
			Name:        "collector-endpoint",
			Value:       metadata_repository.DefaultTelmetryCollectorEndpoint,
			Usage:       "Collector endpoint",
			EnvVars:     []string{"COLLECTOR_ENDPOINT"},
			Destination: &options.CollectorEndpoint,
		},
	}
}
