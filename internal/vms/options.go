package vms

import (
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultRPCBindAddress = "127.0.0.1:9090"

	DefaultClusterID                    = types.ClusterID(1)
	DefaultReplicationFactor            = 1
	DefaultInitialMRConnectRetryCount   = -1
	DefaultInitialMRConnectRetryBackoff = 100 * time.Millisecond

	DefaultTick             = 100 * time.Millisecond
	DefaultReportInterval   = 10
	DefaultHeartbeatTimeout = 10
	DefaultGCTimeout        = 24 * time.Hour
)

type Options struct {
	RPCOptions
	MRManagerOptions
	WatcherOptions

	ClusterID         types.ClusterID
	ReplicationFactor uint

	Verbose bool
	Logger  *zap.Logger
}

type RPCOptions struct {
	RPCBindAddress string
}

type MRManagerOptions struct {
	MetadataRepositoryAddresses []string
	InitialMRConnRetryCount     int
	InitialMRConnRetryBackoff   time.Duration
}

// ReportInterval    : tick time * ReportInterval
// Heartbeat         : check every tick time
// Heartbeat timeout : tick time * HeartbeatTimeout
type WatcherOptions struct {
	Tick             time.Duration
	ReportInterval   int
	HeartbeatTimeout int
	GCTimeout        time.Duration
}

func DefaultOptions() Options {
	return Options{
		RPCOptions: RPCOptions{RPCBindAddress: DefaultRPCBindAddress},
		WatcherOptions: WatcherOptions{
			Tick:             DefaultTick,
			ReportInterval:   DefaultReportInterval,
			HeartbeatTimeout: DefaultHeartbeatTimeout,
			GCTimeout:        DefaultGCTimeout,
		},
		MRManagerOptions: MRManagerOptions{
			InitialMRConnRetryCount:   DefaultInitialMRConnectRetryCount,
			InitialMRConnRetryBackoff: DefaultInitialMRConnectRetryBackoff,
		},
		ClusterID:         DefaultClusterID,
		ReplicationFactor: DefaultReplicationFactor,
		Logger:            zap.NewNop(),
	}
}
