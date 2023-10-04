package flags

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/pkg/types"
)

const (
	CategoryCluster = "Cluster:"

	DefaultClusterID = types.MinClusterID

	DefaultReplicationFactor = 1
)

var (
	ClusterID = &cli.IntFlag{
		Name:     "cluster-id",
		Aliases:  []string{"cluster", "cid"},
		Category: CategoryCluster,
		EnvVars:  []string{"CLUSTER_ID"},
		Value:    int(DefaultClusterID),
		Action: func(_ *cli.Context, value int) error {
			if value < int(types.MinClusterID) || value > int(types.MaxClusterID) {
				return fmt.Errorf("invalid value \"%d\" for flag --cluster-id", value)
			}
			return nil
		},
	}

	ReplicationFactor = &cli.IntFlag{
		Name:     "replication-factor",
		Category: CategoryCluster,
		EnvVars:  []string{"REPLICATION_FACTOR"},
		Value:    DefaultReplicationFactor,
		Action: func(_ *cli.Context, value int) error {
			if value <= 0 {
				return fmt.Errorf("invalid value \"%d\" for flag --replication-factor", value)
			}
			return nil
		},
	}
)
