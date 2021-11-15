package app

import (
	"github.com/docker/go-units"
	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/pkg/benchmark"
)

func New() *cli.App {
	app := &cli.App{
		Name:    "benchmark",
		Version: "v0.0.1",
	}
	app.Commands = append(app.Commands,
		newStartCommand(),
	)
	return app
}

func newStartCommand() *cli.Command {
	cmd := &cli.Command{
		Name:    "start",
		Aliases: []string{"s"},
		Action:  main,
	}
	cmd.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "cluster-id",
			Aliases: []string{"cid"},
			EnvVars: []string{"CLUSTER_ID"},
			Usage:   "cluster identifier",
		},
		&cli.StringSliceFlag{
			Name:     "metadata-repository-address",
			Aliases:  []string{"mraddr"},
			EnvVars:  []string{"METADATA_REPOSITORY_ADDRESS"},
			Usage:    "metadata repository adddress",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "clients",
			Usage: "the number of clients",
			Value: benchmark.DefaultNumClients,
		},
		&cli.StringFlag{
			Name:        "data-size",
			Usage:       "data size in bytes",
			DefaultText: units.BytesSize(float64(benchmark.DefaultDataSizeByte)),
			Value:       units.BytesSize(float64(benchmark.DefaultDataSizeByte)),
		},
		&cli.IntFlag{
			Name:  "report-interval",
			Usage: "report interval in operations",
			Value: benchmark.DefaultReportIntervalOps,
		},
		&cli.IntFlag{
			Name:    "max-operations-per-client",
			Aliases: []string{"max-ops-per-client"},
			Value:   benchmark.DefaultMaxOperationsPerClient,
		},
		&cli.DurationFlag{
			Name:        "max-duration",
			DefaultText: benchmark.DefaultMaxDuration.String(),
			Value:       benchmark.DefaultMaxDuration,
		},
	}
	return cmd
}
