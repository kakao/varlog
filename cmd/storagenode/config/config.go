package config

import (
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type Config struct {
	ClusterID        types.ClusterID
	StorageNodeID    types.StorageNodeID
	Volumes          []storagenode.Volume
	ListenAddress    string
	AdvertiseAddress string
	TelemetryName    string
}
