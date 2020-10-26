package app

import (
	"fmt"
	"net"

	"github.com/urfave/cli/v2"
	"github.com/kakao/varlog/internal/vms"
	"github.com/kakao/varlog/pkg/varlog/types"
)

func InitCLI(options *vms.Options) *cli.App {
	app := &cli.App{
		Name:    "vms",
		Usage:   "run varlog manager server",
		Version: "v0.0.1",
	}
	cli.VersionFlag = &cli.BoolFlag{Name: "version"}
	app.Commands = append(app.Commands, initStartCommand(options))
	return app
}

func initStartCommand(options *vms.Options) *cli.Command {
	startCmd := &cli.Command{
		Name:    "start",
		Aliases: []string{"s"},
		Usage:   "start [flags]",
		Action: func(c *cli.Context) error {
			// ClusterID
			parsedClusterID := c.Uint("cluster-id")
			clusterID, err := types.NewClusterIDFromUint(parsedClusterID)
			if err != nil {
				return err
			}
			options.ClusterID = clusterID

			// Metadata Repository Addresses
			parsedMRAddrs := c.StringSlice("mr-address")
			for _, addr := range parsedMRAddrs {
				host, _, err := net.SplitHostPort(addr)
				if err != nil {
					return fmt.Errorf("invalid metadata repository address %v: %v", addr, err)
				}
				if ip := net.ParseIP(host); ip == nil {
					return fmt.Errorf("invalid metadata repository address: %v", addr)
				}
			}
			options.MetadataRepositoryAddresses = parsedMRAddrs
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

	startCmd.Flags = append(startCmd.Flags,
		&cli.UintFlag{
			Name:    "cluster-id",
			Aliases: []string{},
			Value:   uint(vms.DefaultClusterID),
			Usage:   "cluster id",
			EnvVars: []string{"CLUSTER_ID"},
		},
		&cli.UintFlag{
			Name:        "replication-factor",
			Aliases:     []string{},
			Value:       vms.DefaultReplicationFactor,
			Usage:       "replication factor",
			EnvVars:     []string{"REPLICATION_FACTOR"},
			Destination: &options.ReplicationFactor,
		},
		&cli.StringSliceFlag{
			Name:        "mr-address",
			Aliases:     []string{},
			Usage:       "metadata repository address",
			EnvVars:     []string{"MR_ADDRESS"},
			Destination: cli.NewStringSlice(),
		},
	)

	return startCmd
}

func initRPCFlags(options *vms.RPCOptions) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "rpc-bind-address",
			Aliases:     []string{},
			Value:       vms.DefaultRPCBindAddress,
			Usage:       "RPC bind address",
			EnvVars:     []string{"RPC_BIND_ADDRESS"},
			Destination: &options.RPCBindAddress,
		},
	}
}
