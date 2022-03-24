package app

import (
	"github.daumkakao.com/varlog/varlog/internal/flags"
)

var (
	flagClusterID          = flags.ClusterID()
	flagMetadataRepository = flags.MetadataRepositoryAddress()
	flagListen             = flags.FlagDesc{
		Name:    "listen",
		Aliases: []string{"listen-address", "rpc-bind-address"},
		Envs:    []string{"LISTEN", "LISTEN_ADDRESS", "RPC_BIND_ADDRESS"},
	}
	flagReplicationFactor = flags.FlagDesc{
		Name:  "replication-factor",
		Usage: "replication factor",
		Envs:  []string{"REPLICATION_FACTOR"},
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
	flagSNWatcherRPCTimeout = flags.FlagDesc{
		Name:  "sn-watcher-rpc-timeout",
		Usage: "sn watcher rpc timeout",
		Envs:  []string{"SN_WATCHER_RPC_TIMEOUT"},
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
	flagLogDir = flags.FlagDesc{
		Name:    "logdir",
		Aliases: []string{"log-dir"},
		Envs:    []string{"LOG_DIR", "LOGDIR"},
	}
	flagLogToStderr = flags.FlagDesc{
		Name:    "logtostderr",
		Aliases: []string{"log-to-stderr"},
		Envs:    []string{"LOGTOSTDERR", "LOG_TO_STDERR"},
	}
)
