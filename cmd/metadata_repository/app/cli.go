package app

import (
	"github.com/urfave/cli/v2"
	"github.com/kakao/varlog/internal/metadata_repository"
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
			options.Peers = c.StringSlice("peers")
			return Main(options)
		},
	}

	startCmd.Flags = []cli.Flag{
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
	}

	app.Commands = append(app.Commands, startCmd)
	return app
}
