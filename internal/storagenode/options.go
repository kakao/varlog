package storagenode

import (
	"errors"
	"os"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/timeutil"
	"go.uber.org/zap"
)

const (
	DefaultRPCBindAddress = "127.0.0.1:9091"
)

var (
	DefaultClusterID     = types.ClusterID(1)
	DefaultStorageNodeID = types.StorageNodeID(1)
	DefaultVolume        = Volume(os.TempDir())
	DefaultStorageName   = PebbleStorageName
)

const (
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

var DefaultOptions = Options{
	RPCOptions:               DefaultRPCOptions,
	LogStreamExecutorOptions: DefaultLogStreamExecutorOptions,
	LogStreamReporterOptions: DefaultLogStreamReporterOptions,
	ClusterID:                DefaultClusterID,
	StorageNodeID:            DefaultStorageNodeID,
	Volumes:                  map[Volume]struct{}{DefaultVolume: {}},
	StorageName:              DefaultStorageName,
	Verbose:                  false,
	Logger:                   zap.NewNop(),
}

var DefaultRPCOptions = RPCOptions{RPCBindAddress: DefaultRPCBindAddress}

var DefaultLogStreamExecutorOptions = LogStreamExecutorOptions{
	AppendCSize:       DefaultLSEAppendCSize,
	AppendCTimeout:    DefaultLSEAppendCTimeout,
	CommitWaitTimeout: DefaultLSECommitWaitTimeout,
	TrimCSize:         DefaultLSETrimCSize,
	TrimCTimeout:      DefaultLSETrimCTimeout,
	CommitCSize:       DefaultLSECommitCSize,
	CommitCTimeout:    DefaultLSECommitCTimeout,
}

var DefaultLogStreamReporterOptions = LogStreamReporterOptions{
	CommitCSize:       DefaultLSRCommitCSize,
	CommitCTimeout:    DefaultLSRCommitCTimeout,
	ReportCSize:       DefaultLSRReportCSize,
	ReportCTimeout:    DefaultLSRReportCTimeout,
	ReportWaitTimeout: DefaultLSRReportWaitTimeout,
}

type Options struct {
	RPCOptions
	LogStreamExecutorOptions
	LogStreamReporterOptions

	ClusterID     types.ClusterID
	StorageNodeID types.StorageNodeID
	Volumes       map[Volume]struct{}
	StorageName   string

	Verbose bool

	Logger *zap.Logger
}

type RPCOptions struct {
	RPCBindAddress string
}

type LogStreamExecutorOptions struct {
	AppendCSize       uint
	AppendCTimeout    time.Duration
	CommitWaitTimeout time.Duration

	TrimCSize    uint
	TrimCTimeout time.Duration

	CommitCSize    uint
	CommitCTimeout time.Duration
}

type LogStreamReporterOptions struct {
	CommitCSize    uint
	CommitCTimeout time.Duration

	ReportCSize       uint
	ReportCTimeout    time.Duration
	ReportWaitTimeout time.Duration
}

func (opts Options) Valid() error {
	if len(opts.Volumes) == 0 {
		return errors.New("no volume")
	}
	for volume := range opts.Volumes {
		if err := volume.Valid(); err != nil {
			return err
		}
	}
	if err := ValidStorageName(opts.StorageName); err != nil {
		return err
	}
	if opts.Logger == nil {
		return errors.New("nil logger")
	}
	return nil
}
