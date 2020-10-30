package app

import (
	"time"

	"github.com/kakao/varlog/pkg/varlog/types"
)

const (
	DefaultVMSAddress = "127.0.0.1:9001"
	DefaultTimeout    = time.Second * 5
	DefaultPrinter    = "json"
	DefaultVerbose    = false
)

type Options struct {
	VMSAddress string
	Timeout    time.Duration
	Output     string
	Verbose    bool
}

type AddStorageNodeOption struct {
	StorageNodeAddress string
}

type AddLogStreamOption struct {
	StorageNodeID types.StorageNodeID
}

type RemoveStorageNodeOption struct {
	StorageNodeID types.StorageNodeID
}

type RemoveLogStreamOption struct {
	LogStreamID types.LogStreamID
}

type SealLogStreamOption struct {
	LogStreamID types.LogStreamID
}

type UnsealLogStreamOption struct {
	LogStreamID types.LogStreamID
}

type SyncLogStreamOption struct {
	LogStreamID      types.LogStreamID
	SrcStorageNodeID types.StorageNodeID
	DstStorageNodeID types.StorageNodeID
}
