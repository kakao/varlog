package vms

import (
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"go.uber.org/zap"
)

const (
	DefaultRPCBindAddress = "0.0.0.0:9100"

	DefaultClusterID         = types.ClusterID(1)
	DefaultReplicationFactor = 1

	DefaultTick             = 100 * time.Millisecond
	DefaultReportInterval   = 10
	DefaultHeartbeatTimeout = 10
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
}

var DefaultOptions = Options{
	RPCOptions: RPCOptions{RPCBindAddress: DefaultRPCBindAddress},
	WatcherOptions: WatcherOptions{
		Tick:             DefaultTick,
		ReportInterval:   DefaultReportInterval,
		HeartbeatTimeout: DefaultHeartbeatTimeout,
	},
	ClusterID:         DefaultClusterID,
	ReplicationFactor: DefaultReplicationFactor,
	Logger:            zap.NewNop(),
}
