package config

import (
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/pkg/types"
)

type Config struct {
	ClusterID        types.ClusterID
	StorageNodeID    types.StorageNodeID
	Volumes          []storagenode.Volume
	ListenAddress    string
	AdvertiseAddress string
	TelemetryName    string
}
