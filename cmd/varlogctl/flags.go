package main

var (
	flagAdminAddress = flagDesc{
		name:    "admin-address",
		aliases: []string{"admin-addr", "admin"},
		envs:    []string{"ADMIN_ADDRESS"},
	}
	flagTimeout = flagDesc{
		name: "timeout",
		envs: []string{"TIMEOUT"},
	}
	flagPrettyPrint = flagDesc{
		name: "pretty",
	}
	flagVerbose = flagDesc{
		name: "verbose",
		envs: []string{"VERBOSE"},
	}

	flagMRRaftURL = flagDesc{
		name: "raft-url",
	}
	flagMRRPCAddr = flagDesc{
		name: "rpc-addr",
	}

	flagSNAddr = flagDesc{
		name:    "storage-node-address",
		aliases: []string{"address", "addr"},
	}
	flagStorageNodeID = flagDesc{
		name:    "storage-node-id",
		aliases: []string{"storagenode-id", "snid"},
	}

	flagTopicID = flagDesc{
		name:    "topic-id",
		aliases: []string{"tpid"},
	}

	flagLogStreamID = flagDesc{
		name:    "log-stream-id",
		aliases: []string{"logstream-id", "lsid"},
	}
	flagSyncSrc = flagDesc{
		name: "src",
	}
	flagSyncDst = flagDesc{
		name: "dst",
	}
)
