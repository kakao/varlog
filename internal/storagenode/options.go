package storagenode

import (
	"errors"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/timeutil"
)

const (
	DefaultListenAddress = "0.0.0.0:9091"
)

var (
	DefaultClusterID     = types.ClusterID(1)
	DefaultStorageNodeID = types.StorageNodeID(1)
	DefaultVolume        = Volume(os.TempDir())

	DefaultTelemetryCollectorName    = "nop"
	DefaultTelmetryCollectorEndpoint = "" // "http://localhost:14268/api/traces"
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

func DefaultOptions() Options {
	return Options{
		RPCOptions:               DefaultRPCOptions(),
		LogStreamExecutorOptions: DefaultLogStreamExecutorOptions(),
		LogStreamReporterOptions: DefaultLogStreamReporterOptions(),
		StorageOptions:           DefaultStorageOptions(),
		PProfServerConfig:        defaultPProfServerConfig(),
		TelemetryOptions:         DefaultTelemetryOptions(),
		ClusterID:                DefaultClusterID,
		StorageNodeID:            DefaultStorageNodeID,
		Volumes:                  map[Volume]struct{}{DefaultVolume: {}},
		Verbose:                  false,
		Logger:                   zap.NewNop(),
	}
}

func DefaultRPCOptions() RPCOptions {
	return RPCOptions{ListenAddress: DefaultListenAddress}
}

func DefaultLogStreamExecutorOptions() LogStreamExecutorOptions {
	return LogStreamExecutorOptions{
		AppendCSize:       DefaultLSEAppendCSize,
		AppendCTimeout:    DefaultLSEAppendCTimeout,
		CommitWaitTimeout: DefaultLSECommitWaitTimeout,
		TrimCSize:         DefaultLSETrimCSize,
		TrimCTimeout:      DefaultLSETrimCTimeout,
		CommitCSize:       DefaultLSECommitCSize,
		CommitCTimeout:    DefaultLSECommitCTimeout,
	}
}

func DefaultLogStreamReporterOptions() LogStreamReporterOptions {
	return LogStreamReporterOptions{
		CommitCSize:       DefaultLSRCommitCSize,
		CommitCTimeout:    DefaultLSRCommitCTimeout,
		ReportCSize:       DefaultLSRReportCSize,
		ReportCTimeout:    DefaultLSRReportCTimeout,
		ReportWaitTimeout: DefaultLSRReportWaitTimeout,
	}
}

func DefaultTelemetryOptions() TelemetryOptions {
	return TelemetryOptions{
		CollectorName:     DefaultTelemetryCollectorName,
		CollectorEndpoint: DefaultTelmetryCollectorEndpoint,
	}
}

type Options struct {
	RPCOptions
	LogStreamExecutorOptions
	LogStreamReporterOptions
	StorageOptions
	TelemetryOptions
	PProfServerConfig

	ClusterID     types.ClusterID
	StorageNodeID types.StorageNodeID
	Volumes       map[Volume]struct{}

	Verbose bool

	Logger *zap.Logger
}

// ListenAddress
// AdvertiseAddress
type RPCOptions struct {
	ListenAddress    string
	AdvertiseAddress string
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

type TelemetryOptions struct {
	CollectorName     string
	CollectorEndpoint string
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
	if err := ValidStorageName(opts.StorageOptions.Name); err != nil {
		return err
	}
	if opts.Logger == nil {
		return errors.New("nil logger")
	}
	return nil
}
