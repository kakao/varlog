package storagenode

import (
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/timeutil"
	"go.uber.org/zap"
)

const (
	DefaultRPCBindAddress = "0.0.0.0:9091"

	DefaultLSEAppendCSize       = uint(0)
	DefaultLSEAppendCTimeout    = timeutil.MaxDuration
	DefaultLSECommitWaitTimeout = timeutil.MaxDuration

	DefaultLSECommitCSize    = uint(0)
	DefaultLSECommitCTimeout = timeutil.MaxDuration

	DefaultLSETrimCSize    = uint(0)
	DefaultLSETrimCTimeout = timeutil.MaxDuration
)

const (
	DefaultLSRCommitCSize    = uint(0)
	DefaultLSRCommitCTimeout = timeutil.MaxDuration

	DefaultLSRReportCSize       = uint(0)
	DefaultLSRReportCTimeout    = timeutil.MaxDuration
	DefaultLSRReportWaitTimeout = timeutil.MaxDuration
)

type StorageNodeOptions struct {
	RPCOptions
	LogStreamExecutorOptions
	LogStreamReporterOptions

	ClusterID     types.ClusterID
	StorageNodeID types.StorageNodeID

	Verbose bool

	Logger *zap.Logger
}

type RPCOptions struct {
	RPCBindAddress string
}

var DefaultRPCOptions = RPCOptions{RPCBindAddress: DefaultRPCBindAddress}

type LogStreamExecutorOptions struct {
	AppendCSize       uint
	AppendCTimeout    time.Duration
	CommitWaitTimeout time.Duration

	TrimCSize    uint
	TrimCTimeout time.Duration

	CommitCSize    uint
	CommitCTimeout time.Duration
}

var DefaultLogStreamExecutorOptions = LogStreamExecutorOptions{
	AppendCSize:       DefaultLSEAppendCSize,
	AppendCTimeout:    DefaultLSEAppendCTimeout,
	CommitWaitTimeout: DefaultLSECommitWaitTimeout,
	TrimCSize:         DefaultLSETrimCSize,
	TrimCTimeout:      DefaultLSETrimCTimeout,
	CommitCSize:       DefaultLSECommitCSize,
	CommitCTimeout:    DefaultLSECommitCTimeout,
}

type LogStreamReporterOptions struct {
	CommitCSize    uint
	CommitCTimeout time.Duration

	ReportCSize       uint
	ReportCTimeout    time.Duration
	ReportWaitTimeout time.Duration
}

var DefaultLogStreamReporterOptions = LogStreamReporterOptions{
	CommitCSize:       DefaultLSRCommitCSize,
	CommitCTimeout:    DefaultLSRCommitCTimeout,
	ReportCSize:       DefaultLSRReportCSize,
	ReportCTimeout:    DefaultLSRReportCTimeout,
	ReportWaitTimeout: DefaultLSRReportWaitTimeout,
}
