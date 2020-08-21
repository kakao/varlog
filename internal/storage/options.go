package storage

import (
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

var (
	DefaultRPCBindAddress = "0.0.0.0:9091"

	DefaultLSEAppendCSize    = uint32(0)
	DefaultLSEAppendCTimeout = time.Duration(0)

	DefaultLSECommitCSize    = uint32(0)
	DefaultLSECommitCTimeout = time.Duration(0)

	DefaultLSETrimCSize    = uint32(0)
	DefaultLSETrimCTimeout = time.Duration(0)
)

type StorageNodeOptions struct {
	RPCOptions
	LogStreamExecutorOptions

	ClusterID     types.ClusterID
	StorageNodeID types.StorageNodeID
}

type RPCOptions struct {
	BindAddress string
}

type LogStreamExecutorOptions struct {
	AppendCSize    uint32
	AppendCTimeout time.Duration

	TrimCSize    uint32
	TrimCTimeout time.Duration

	CommitCSize    uint32
	CommitCTimeout time.Duration
}
