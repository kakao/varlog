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

// GetClusterIDFlag returns a flag for specifying the Cluster ID. Users can
// modify the returned flag without affecting the predefined flag.
func GetStorageNodeIDFlag() *cli.IntFlag {
	return &cli.IntFlag{
		Name:     "storagenode-id",
		Category: CategoryCluster,
		Aliases:  []string{"storage-node-id", "snid"},
		EnvVars:  []string{"STORAGENODE_ID", "STORAGE_NODE_ID", "SNID"},
		Usage:    "StorageNode ID",
		Action: func(c *cli.Context, value int) error {
			if c.IsSet("storagenode-id") && types.StorageNodeID(value).Invalid() {
				return fmt.Errorf("invalid value \"%d\" for flag --storagenode-id", value)
			}
			return nil
		},
	}
}

// GetTopicIDFlag returns a flag for specifying the Topic ID. Users can modify
// the returned flag without affecting the predefined flag.
func GetTopicIDFlag() *cli.IntFlag {
	return &cli.IntFlag{
		Name:     "topic-id",
		Category: CategoryCluster,
		Aliases:  []string{"topic", "tpid"},
		EnvVars:  []string{"TOPIC_ID", "TOPIC", "TPID"},
		Usage:    "Topic ID",
		Action: func(c *cli.Context, value int) error {
			if c.IsSet("topic-id") && types.TopicID(value).Invalid() {
				return fmt.Errorf("invalid value \"%d\" for flag --topic-id", value)
			}
			return nil
		},
	}
}

// GetLogStreamIDFlag returns a flag for specifying the LogStream ID. Users can
// modify the returned flag without affecting the predefined flag.
func GetLogStreamIDFlag() *cli.IntFlag {
	return &cli.IntFlag{
		Name:     "logstream-id",
		Category: CategoryCluster,
		Aliases:  []string{"log-stream-id", "logstream", "lsid"},
		EnvVars:  []string{"LOGSTREAM_ID", "LOG_STREAM_ID", "LOGSTREAM", "LSID"},
		Usage:    "LogStream ID",
		Action: func(c *cli.Context, value int) error {
			if c.IsSet("logstream-id") && types.LogStreamID(value).Invalid() {
				return fmt.Errorf("invalid value \"%d\" for flag --logstream-id", value)
			}
			return nil
		},
	}
}
