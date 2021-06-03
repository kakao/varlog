package app

import (
	"github.com/urfave/cli/v2"

	"github.daumkakao.com/varlog/varlog/cmd/storagenode/config"
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

var (
	clusterIDUint      uint
	storageNodeIDUint  uint
	volumesStringSlice = cli.NewStringSlice()
	listenAddress      string
	advertiseAddress   string
	telemetryName      string
)

func InitCLI(cfg *config.Config) *cli.App {
	app := &cli.App{
		Name:    "storagenode",
		Usage:   "run storage node",
		Version: "v0.0.1",
	}

	cli.VersionFlag = &cli.BoolFlag{Name: "version"}

	app.Commands = append(app.Commands, initStartCommand(cfg))
	return app
}

func initStartCommand(cfg *config.Config) *cli.Command {
	startCmd := &cli.Command{
		Name:    "start",
		Aliases: []string{"s"},
		Usage:   "start [flags]",
		Action: func(c *cli.Context) error {
			var err error

			// ClusterID
			if cfg.ClusterID, err = types.NewClusterIDFromUint(clusterIDUint); err != nil {
				return err
			}

			// StorageNodeID
			if cfg.StorageNodeID, err = types.NewStorageNodeIDFromUint(storageNodeIDUint); err != nil {
				return err
			}

			// Volumes
			for _, vol := range volumesStringSlice.Value() {
				volume, err := storagenode.NewVolume(vol)
				if err != nil {
					return err
				}
				cfg.Volumes = append(cfg.Volumes, volume)
			}

			// Addresses
			cfg.ListenAddress = listenAddress
			cfg.AdvertiseAddress = advertiseAddress

			return Main(cfg)
		},
	}

	startCmd.Flags = []cli.Flag{
		&cli.UintFlag{
			Name:        "cluster-id",
			Aliases:     []string{"cid"},
			Usage:       "cluster id",
			EnvVars:     []string{"CLUSTER_ID"},
			Destination: &clusterIDUint,
		},
		&cli.UintFlag{
			Name:        "storage-node-id",
			Aliases:     []string{"snid"},
			Usage:       "storage node id",
			EnvVars:     []string{"STORAGE_NODE_ID"},
			Destination: &storageNodeIDUint,
		},
		&cli.StringSliceFlag{
			Name:        "volumes",
			Aliases:     []string{},
			Usage:       "volumes",
			EnvVars:     []string{"VOLUMES"},
			Destination: volumesStringSlice,
		},
		&cli.StringFlag{
			Name:        "listen-address",
			Aliases:     []string{"rpc-bind-address"},
			Usage:       "RPC listen address",
			EnvVars:     []string{"LISTEN_ADDRESS, RPC_BIND_ADDRESS"},
			Destination: &listenAddress,
		},
		&cli.StringFlag{
			Name:        "advertise-address",
			Usage:       "RPC advertise address",
			EnvVars:     []string{"ADVERTISE_ADDRESS"},
			Destination: &advertiseAddress,
		},
		&cli.StringFlag{
			Name:        "collector-name",
			Value:       "nop",
			Usage:       "Collector name",
			EnvVars:     []string{"COLLECTOR_NAME"},
			Destination: &telemetryName,
		},
	}
	return startCmd
}
