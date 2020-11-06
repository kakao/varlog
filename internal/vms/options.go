package vms

import (
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

const (
	DefaultRPCBindAddress = "127.0.0.1:9090"

	DefaultClusterID         = types.ClusterID(1)
	DefaultReplicationFactor = 1

	DefaultTick             = 100 * time.Millisecond
	DefaultReportInterval   = 10
	DefaultHeartbeatTimeout = 10
	DefaultGCTimeout        = 24 * time.Hour
)

type Options struct {
	RPCOptions
	WatcherOptions

	ClusterID                   types.ClusterID
	ReplicationFactor           uint
	MetadataRepositoryAddresses []string

	Verbose bool
	Logger  *zap.Logger
}

type RPCOptions struct {
	RPCBindAddress string
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

var DefaultOptions = Options{
	RPCOptions: RPCOptions{RPCBindAddress: DefaultRPCBindAddress},
	WatcherOptions: WatcherOptions{
		Tick:             DefaultTick,
		ReportInterval:   DefaultReportInterval,
		HeartbeatTimeout: DefaultHeartbeatTimeout,
		GCTimeout:        DefaultGCTimeout,
	},
	ClusterID:         DefaultClusterID,
	ReplicationFactor: DefaultReplicationFactor,
	Logger:            zap.NewNop(),
}
