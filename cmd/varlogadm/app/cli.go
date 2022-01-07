package app

import (
	"fmt"
	"net"

	"github.com/urfave/cli/v2"

	"github.daumkakao.com/varlog/varlog/internal/varlogadm"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func InitCLI(options *varlogadm.Options) *cli.App {
	app := &cli.App{
		Name:    "varlogadm",
		Usage:   "run varlog manager server",
		Version: "v0.0.1",
	}
	cli.VersionFlag = &cli.BoolFlag{Name: "version"}
	app.Commands = append(app.Commands, initStartCommand(options))
	return app
}

func initStartCommand(options *varlogadm.Options) *cli.Command {
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
	startCmd.Flags = append(startCmd.Flags,
		&cli.UintFlag{
			Name:    "cluster-id",
			Aliases: []string{},
			Value:   uint(varlogadm.DefaultClusterID),
			Usage:   "cluster id",
			EnvVars: []string{"CLUSTER_ID"},
		},
		&cli.StringFlag{
			Name:        "listen-address",
			Aliases:     []string{"rpc-bind-address"},
			Value:       varlogadm.DefaultListenAddress,
			Usage:       "RPC listemn address",
			EnvVars:     []string{"LISTEN_ADDRESS", "RPC_BIND_ADDRESS"},
			Destination: &options.ListenAddress,
		},
		&cli.UintFlag{
			Name:        "replication-factor",
			Aliases:     []string{},
			Value:       varlogadm.DefaultReplicationFactor,
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
		&cli.IntFlag{
			Name:        "init-mr-conn-retry-count",
			Aliases:     []string{},
			Value:       varlogadm.DefaultInitialMRConnectRetryCount,
			Usage:       "the number of retry of initial metadata repository connect",
			EnvVars:     []string{"INIT_MR_CONN_RETRY_COUNT"},
			Destination: &options.InitialMRConnRetryCount,
		},
		&cli.DurationFlag{
			Name:        "init-mr-conn-retry-backoff",
			Aliases:     []string{},
			Value:       varlogadm.DefaultInitialMRConnectRetryBackoff,
			Usage:       "backoff duration between retries of initial metadata repository connect",
			EnvVars:     []string{"INIT_MR_CONN_RETRY_BACKOFF"},
			Destination: &options.InitialMRConnRetryBackoff,
		},
		&cli.DurationFlag{
			Name:        "sn-watcher-rpc-timeout",
			Aliases:     []string{},
			Value:       varlogadm.DefaultWatcherRPCTimeout,
			Usage:       "sn watcher rpc timeout",
			EnvVars:     []string{"SN_WATCHER_RPC_TIMEOUT"},
			Destination: &options.WatcherOptions.RPCTimeout,
		},
		&cli.DurationFlag{
			Name:        "mr-conn-timeout",
			Aliases:     []string{},
			Value:       varlogadm.DefaultMRConnTimeout,
			Usage:       "mr connection timeout",
			EnvVars:     []string{"MR_CONN_TIMEOUT"},
			Destination: &options.WatcherOptions.RPCTimeout,
		},
		&cli.DurationFlag{
			Name:        "mr-call-timeout",
			Aliases:     []string{},
			Value:       varlogadm.DefaultMRCallTimeout,
			Usage:       "mr call timeout",
			EnvVars:     []string{"MR_CALL_TIMEOUT"},
			Destination: &options.WatcherOptions.RPCTimeout,
		},
	)

	return startCmd
}
