package main

import (
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/admin"
	"github.com/kakao/varlog/internal/flags"
)

var (
	flagClusterID          = flags.ClusterID
	flagMetadataRepository = flags.MetadataRepositoryAddress()
	flagListen             = flags.FlagDesc{
		Name:    "listen",
		Aliases: []string{"listen-address", "rpc-bind-address"},
		Envs:    []string{"LISTEN", "LISTEN_ADDRESS", "RPC_BIND_ADDRESS"},
	}
	flagReplicationFactor  = flags.ReplicationFactor
	flagLogStreamGCTimeout = flags.FlagDesc{
		Name: "log-stream-gc-timeout",
		Envs: []string{"LOG_STREAM_GC_TIMEOUT"},
	}
	flagDisableAutoLogStreamSync = flags.FlagDesc{
		Name: "disable-auto-log-stream-sync",
		Envs: []string{"DISABLE_AUTO_LOG_STREAM_SYNC"},
	}
	flagAutoUnseal = flags.FlagDesc{
		Name:    "auto-unseal",
		Aliases: []string{"enable-auto-unseal", "with-auto-unseal"},
		Envs:    []string{"AUTO_UNSEAL", "ENABLE_AUTO_UNSEAL", "WITH_AUTO_UNSEAL"},
	}
	flagReplicaSelector = &cli.StringFlag{
		Name:    "replica-selector",
		Aliases: []string{"repsel"},
		EnvVars: []string{"REPLICA_SELECTOR"},
		Value:   admin.ReplicaSelectorNameLFU,
		Usage:   strings.Join([]string{admin.ReplicaSelectorNameRandom, admin.ReplicaSelectorNameLFU}, " | "),
	}

	flagInitMRConnRetryCount = flags.FlagDesc{
		Name:  "init-mr-conn-retry-count",
		Usage: "the number of retry of initial metadata repository connect",
		Envs:  []string{"INIT_MR_CONN_RETRY_COUNT"},
	}
	flagInitMRConnRetryBackoff = flags.FlagDesc{
		Name:  "init-mr-conn-retry-backoff",
		Usage: "backoff duration between retries of initial metadata repository connect",
		Envs:  []string{"INIT_MR_CONN_RETRY_BACKOFF"},
	}
	flagMRConnTimeout = flags.FlagDesc{
		Name:  "mr-conn-timeout",
		Usage: "mr connection timeout",
		Envs:  []string{"MR_CONN_TIMEOUT"},
	}
	flagMRCallTimeout = flags.FlagDesc{
		Name:  "mr-call-timeout",
		Usage: "mr call timeout",
		Envs:  []string{"MR_CALL_TIMEOUT"},
	}

	flagSNWatcherHeartbeatCheckDeadline = flags.FlagDesc{
		Name:  "sn-watcher-heartbeat-check-deadline",
		Envs:  []string{"SN_WATCHER_HEARTBEAT_CHECK_DEADLINE"},
		Usage: "dealine for heartbeat check request to storage node",
	}
	flagSNWatcherReportDeadline = flags.FlagDesc{
		Name:  "sn-watcher-report-deadline",
		Envs:  []string{"SN_WATCHER_REPORT_DEADLINE"},
		Usage: "dealine for report request to storage node",
	}
	flagSNWatcherHeartbeatTimeout = flags.FlagDesc{
		Name:  "sn-watcher-heartbeat-timeout",
		Envs:  []string{"SN_WATCHER_HEARTBEAT_TIMEOUT"},
		Usage: "dealine to decide whether a storage node is live",
	}
)
