package app

import "github.com/kakao/varlog/pkg/vflag"

var (
	// flags for storage node
	flagClusterID = vflag.FlagDescriptor{
		Name:        "cluster-id",
		Aliases:     []string{"cid", "clusterid"},
		EnvVars:     []string{"CLUSTER_ID"},
		Description: "Set a identifier for varlog cluster",
	}
	flagStorageNodeID = vflag.FlagDescriptor{
		Name:        "storage-node-id",
		Aliases:     []string{"snid", "storagenodeid"},
		EnvVars:     []string{"STORAGE_NODE_ID", "STORAGENODE_ID"},
		Description: "Set a identifier for storage node",
	}
	flagVolumes = vflag.FlagDescriptor{
		Name:     "volumes",
		Aliases:  []string{"volume", "vol"},
		EnvVars:  []string{"VOLUMES", "VOLUME"},
		Required: true,
	}
	flagListenAddress = vflag.FlagDescriptor{
		Name:     "listen-address",
		Aliases:  []string{"listen"},
		EnvVars:  []string{"LISTEN_ADDRESS", "LISTEN"},
		Required: true,
	}
	flagAdvertiseAddress = vflag.FlagDescriptor{
		Name:    "advertise-address",
		Aliases: []string{"advertise"},
		EnvVars: []string{"ADVERTISE_ADDRESS", "ADVERTISE"},
	}

	// logging
	flagLogDir = vflag.FlagDescriptor{
		Name:    "log-dir",
		EnvVars: []string{"LOG_DIR"},
	}
	flagLogToStderr = vflag.FlagDescriptor{
		Name:    "logtostderr",
		EnvVars: []string{"LOGTOSTDERR"},
	}

	// flags for executors
	flagWriteQueueSize = vflag.FlagDescriptor{
		Name:    "write-queue-size",
		Aliases: []string{"wqsz"},
		EnvVars: []string{"WRITE_QUEUE_SIZE"},
	}
	flagWriteBatchSize = vflag.FlagDescriptor{
		Name:    "write-batch-size",
		Aliases: []string{"wbsz"},
		EnvVars: []string{"WRITE_BATCH_SIZE"},
	}
	flagCommitQueueSize = vflag.FlagDescriptor{
		Name:    "commit-queue-size",
		Aliases: []string{"cqsz"},
		EnvVars: []string{"COMMIT_QUEUE_SIZE"},
	}
	flagCommitBatchSize = vflag.FlagDescriptor{
		Name:    "commit-batch-size",
		Aliases: []string{"cbsz"},
		EnvVars: []string{"COMMIT_BATCH_SIZE"},
	}
	flagReplicateQueueSize = vflag.FlagDescriptor{
		Name:    "replicate-queue-size",
		Aliases: []string{"rqsz"},
		EnvVars: []string{"REPLICATE_QUEUE_SIZE"},
	}

	// flags for storage
	flagDisableWriteSync = vflag.FlagDescriptor{
		Name:    "disable-write-sync",
		Aliases: []string{"without-write-sync", "no-write-sync"},
		EnvVars: []string{"DISABLE_WRITE_SYNC"},
	}
	flagDisableCommitSync = vflag.FlagDescriptor{
		Name:    "disable-commit-sync",
		Aliases: []string{"without-commit-sync", "no-commit-sync"},
		EnvVars: []string{"DISABLE_COMMIT_SYNC"},
	}
	flagDisableDeleteCommittedSync = vflag.FlagDescriptor{
		Name:    "disable-delete-committed-sync",
		Aliases: []string{"without-delete-committed-sync", "no-delete-committed-sync"},
		EnvVars: []string{"DISABLE_DELETE_COMMITTED_SYNC"},
	}
	flagDisableDeleteUncommittedSync = vflag.FlagDescriptor{
		Name:    "disable-delete-uncommitted-sync",
		Aliases: []string{"without-delete-uncommited-sync", "no-delete-uncommitted-sync"},
		EnvVars: []string{"DISABLE_DELETE_UNCOMMITTED_SYNC"},
	}
	flagMemTableSizeBytes = vflag.FlagDescriptor{
		Name:    "memtable-size-bytes",
		EnvVars: []string{"MEMTABLE_SIZE_BYTES"},
	}
	flagMemTableStopWritesThreshold = vflag.FlagDescriptor{
		Name:    "memtable-stop-writes-threshold",
		EnvVars: []string{"MEMTABLE_STOP_WRITES_THRESHOLD"},
	}
	flagStorageDebugLog = vflag.FlagDescriptor{
		Name:    "storage-debug-log",
		EnvVars: []string{"STORAGE_DEBUG_LOG"},
	}

	// flags for telemetry
	flagTelemetry = vflag.FlagDescriptor{
		Name:    "telemetry",
		EnvVars: []string{"TELEMETRY"},
	}
)
