package vms

import (
	"github.com/kakao/varlog/pkg/varlog/types"
	"go.uber.org/zap"
)

const (
	DefaultRPCBindAddress = "0.0.0.0:9100"

	DefaultClusterID         = types.ClusterID(1)
	DefaultReplicationFactor = 1
)

type Options struct {
	RPCOptions

	ClusterID                   types.ClusterID
	ReplicationFactor           uint
	MetadataRepositoryAddresses []string

	Verbose bool
	Logger  *zap.Logger
}

type RPCOptions struct {
	RPCBindAddress string
}

var DefaultOptions = Options{
	RPCOptions:        RPCOptions{RPCBindAddress: DefaultRPCBindAddress},
	ClusterID:         DefaultClusterID,
	ReplicationFactor: DefaultReplicationFactor,
	Logger:            zap.NewNop(),
}
