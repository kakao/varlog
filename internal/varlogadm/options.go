package varlogadm

import (
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultListenAddress     = "127.0.0.1:9090"
	DefaultWatcherRPCTimeout = 3 * time.Second

	DefaultClusterID                    = types.ClusterID(1)
	DefaultReplicationFactor            = 1
	DefaultInitialMRConnectRetryCount   = -1
	DefaultInitialMRConnectRetryBackoff = 100 * time.Millisecond
	DefaultMRConnTimeout                = 1 * time.Second
	DefaultMRCallTimeout                = 3 * time.Second

	DefaultTick             = 100 * time.Millisecond
	DefaultReportInterval   = 10
	DefaultHeartbeatTimeout = 10
	DefaultGCTimeout        = 24 * time.Hour
)

type Options struct {
	MRManagerOptions
	WatcherOptions

	ListenAddress     string
	ClusterID         types.ClusterID
	ReplicationFactor uint

	Verbose bool
	Logger  *zap.Logger
}

type MRManagerOptions struct {
	MetadataRepositoryAddresses []string
	InitialMRConnRetryCount     int
	InitialMRConnRetryBackoff   time.Duration
	ConnTimeout                 time.Duration
	CallTimeout                 time.Duration
}

// ReportInterval    : tick time * ReportInterval
// Heartbeat         : check every tick time
// Heartbeat timeout : tick time * HeartbeatTimeout
type WatcherOptions struct {
	Tick             time.Duration
	ReportInterval   int
	HeartbeatTimeout int
	GCTimeout        time.Duration
	// timeout for heartbeat and report
	RPCTimeout time.Duration
}

func DefaultOptions() Options {
	return Options{
		WatcherOptions: WatcherOptions{
			Tick:             DefaultTick,
			ReportInterval:   DefaultReportInterval,
			HeartbeatTimeout: DefaultHeartbeatTimeout,
			GCTimeout:        DefaultGCTimeout,
			RPCTimeout:       DefaultWatcherRPCTimeout,
		},
		MRManagerOptions: MRManagerOptions{
			InitialMRConnRetryCount:   DefaultInitialMRConnectRetryCount,
			InitialMRConnRetryBackoff: DefaultInitialMRConnectRetryBackoff,
			ConnTimeout:               DefaultMRConnTimeout,
			CallTimeout:               DefaultMRCallTimeout,
		},
		ListenAddress:     DefaultListenAddress,
		ClusterID:         DefaultClusterID,
		ReplicationFactor: DefaultReplicationFactor,
		Logger:            zap.NewNop(),
	}
}
