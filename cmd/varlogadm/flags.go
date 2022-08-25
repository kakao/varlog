package main

import (
	"github.com/kakao/varlog/internal/flags"
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
		Name: "sn-watcher-heartbeat-check-deadline",
		Envs: []string{"SN_WATCHER_HEARTBEAT_CHECK_DEADLINE"},
	}
	flagSNWatcherReportDeadline = flags.FlagDesc{
		Name: "sn-watcher-report-deadline",
		Envs: []string{"SN_WATCHER_REPORT_DEADLINE"},
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
	flagLogFileRetentionDays = flags.FlagDesc{
		Name:    "logfile-retention-days",
		Aliases: []string{"log-file-retention-days"},
		Envs:    []string{"LOGFILE_RETENTION_DAYS", "LOG_FILE_RETENTION_DAYS"},
	}
	flagLogFileCompression = flags.FlagDesc{
		Name:    "logfile-compression",
		Aliases: []string{"log-file-compression"},
		Envs:    []string{"LOGFILE_COMPRESSION", "LOG_FILE_COMPRESSION"},
	}
	flagLogLevel = flags.FlagDesc{
		Name:    "loglevel",
		Aliases: []string{"log-level"},
		Envs:    []string{"LOGLEVEL", "LOG_LEVEL"},
	}
)
