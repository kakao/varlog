package storage

import (
	"time"

	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/timeutil"
)

const (
	DefaultRPCBindAddress = "0.0.0.0:9091"

	DefaultLSEAppendCSize       = uint32(0)
	DefaultLSEAppendCTimeout    = timeutil.MaxDuration
	DefaultLSECommitWaitTimeout = timeutil.MaxDuration

	DefaultLSECommitCSize    = uint32(0)
	DefaultLSECommitCTimeout = timeutil.MaxDuration

	DefaultLSETrimCSize    = uint32(0)
	DefaultLSETrimCTimeout = timeutil.MaxDuration

	DefaultLSRCommitCSize    = uint32(0)
	DefaultLSRCommitCTimeout = timeutil.MaxDuration

	DefaultLSRReportCSize       = uint32(0)
	DefaultLSRReportCTimeout    = timeutil.MaxDuration
	DefaultLSRReportWaitTimeout = timeutil.MaxDuration
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
	AppendCSize       uint32
	AppendCTimeout    time.Duration
	CommitWaitTimeout time.Duration

	TrimCSize    uint32
	TrimCTimeout time.Duration

	CommitCSize    uint32
	CommitCTimeout time.Duration
}

type LogStreamReporterOptions struct {
	CommitCSize    uint32
	CommitCTimeout time.Duration

	ReportCSize       uint32
	ReportCTimeout    time.Duration
	ReportWaitTimeout time.Duration
}

var DefaultLogStreamReporterOptions = &LogStreamReporterOptions{
	CommitCSize:       DefaultLSRCommitCSize,
	CommitCTimeout:    DefaultLSRCommitCTimeout,
	ReportCSize:       DefaultLSRReportCSize,
	ReportCTimeout:    DefaultLSRReportCTimeout,
	ReportWaitTimeout: DefaultLSRReportWaitTimeout,
}
