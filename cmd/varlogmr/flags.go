package main

import (
	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/internal/metarepos"
)

var (
	flagClusterID = flags.FlagDesc{
		Name:    "cluster-id",
		Aliases: []string{"cid"},
		Usage:   "cluster id",
		Envs:    []string{"CLUSTER_ID"},
	}

	flagRPCAddr = flags.FlagDesc{
		Name:    "rpc-address",
		Aliases: []string{"b", "bind"},
		Usage:   "Bind Address",
		Envs:    []string{"BIND", "RPC_ADDRESS"},
	}

	flagRaftAddr = flags.FlagDesc{
		Name:    "raft-address",
		Aliases: []string{},
		Usage:   "Raft Address",
		Envs:    []string{"RAFT_ADDRESS"},
	}

	flagDebugAddr = flags.FlagDesc{
		Name:    "debug-address",
		Aliases: []string{"d", "debug-addr"},
		Usage:   "Debug Address",
		Envs:    []string{"DEBUG_ADDRESS"},
	}

	flagReplicationFactor = flags.FlagDesc{
		Name:    "log-rep-factor",
		Aliases: []string{"replication-factor"},
		Usage:   "Replication factor or log stream",
		Envs:    []string{"LOG_REP_FACTOR"},
	}

	flagRaftProposeTimeout = flags.FlagDesc{
		Name: "raft-propose-timeout",
		Envs: []string{"RAFT_PROPOSE_TIMEOUT"},
	}

	flagRPCTimeout = flags.FlagDesc{
		Name: "rpc-timeout",
		Envs: []string{"RPC_TIMEOUT"},
	}

	flagCommitTick = flags.FlagDesc{
		Name: "commit-tick",
		Envs: []string{"COMMIT_TICK"},
	}

	flagPromoteTick = flags.FlagDesc{
		Name: "promote-tick",
		Envs: []string{"PROMOTE_TICK"},
	}

	flagJoin = flags.FlagDesc{
		Name:    "join",
		Aliases: []string{},
		Usage:   "join to cluster",
		Envs:    []string{"JOIN"},
	}

	flagSnapshotCount = flags.FlagDesc{
		Name:    "snapshot-count",
		Aliases: []string{},
		Usage:   "Count of entries for Snapshot",
		Envs:    []string{"SNAPSHOT_COUNT"},
	}

	flagMaxSnapshotCatchUpCount = flags.FlagDesc{
		Name:    "max-snapshot-catchup-count",
		Aliases: []string{},
		Envs:    []string{"MAX_SNAPSHOT_CATCHUP_COUNT"},
	}

	flagMaxSnapPurgeCount = flags.FlagDesc{
		Name:    "max-snap-purge-count",
		Aliases: []string{},
		Usage:   "Count of purge files for Snapshot",
		Envs:    []string{"MAX_SNAP_PURGE_COUNT"},
	}

	flagMaxWALPurgeCount = flags.FlagDesc{
		Name:    "max-wal-purge-count",
		Aliases: []string{},
		Usage:   "Count of purge files for WAL",
		Envs:    []string{"MAX_WAL_PURGE_COUNT"},
	}

	flagRaftTick = flags.FlagDesc{
		Name: "raft-tick",
		Envs: []string{"RAFT_TICK"},
	}

	flagRaftDir = flags.FlagDesc{
		Name:    "raft-dir",
		Aliases: []string{},
		Usage:   "Raft Dir",
		Envs:    []string{"RAFT_DIR"},
	}

	flagPeers = flags.FlagDesc{
		Name:    "peers",
		Aliases: []string{"p"},
		Usage:   "Peers of cluster",
		Envs:    []string{"PEERS"},
	}

	flagReportCommitterReadBufferSize = flags.FlagDesc{
		Name: "reportcommitter-read-buffer-size",
		Envs: []string{"REPORTCOMMITTER_READ_BUFFER_SIZE"},
	}

	flagReportCommitterWriteBufferSize = flags.FlagDesc{
		Name: "reportcommitter-write-buffer-size",
		Envs: []string{"REPORTCOMMITTER_WRITE_BUFFER_SIZE"},
	}

	flagMaxTopicsCount = &cli.IntFlag{
		Name:  "max-topics-count",
		Usage: "Maximum number of topics, infinity if it is negative",
		Value: metarepos.DefaultMaxTopicsCount,
	}

	flagTelemetryCollectorName = flags.FlagDesc{
		Name:    "telemetry-collector-name",
		Aliases: []string{"collector-name"},
		Usage:   "Collector name",
		Envs:    []string{"COLLECTOR_NAME"},
	}

	flagTelemetryCollectorEndpoint = flags.FlagDesc{
		Name:    "telemetry-collector-endpoint",
		Aliases: []string{"collector-endpoint"},
		Usage:   "Collector endpoint",
		Envs:    []string{"COLLECTOR_ENDPOINT"},
	}

	flagLogDir = flags.FlagDesc{
		Name:    "log-dir",
		Aliases: []string{},
		Usage:   "Log Dir",
		Envs:    []string{"LOG_DIR"},
	}
)
