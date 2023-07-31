package flags

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

const (
	CategoryCluster = "Cluster:"

	DefaultReplicationFactor = 1
)

var (
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
