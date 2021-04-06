package id

import "github.daumkakao.com/varlog/varlog/pkg/types"

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/id -package id -destination id_mock.go .  ClusterIDGetter,StorageNodeIDGetter

type ClusterIDGetter interface {
	ClusterID() types.ClusterID
}

type StorageNodeIDGetter interface {
	StorageNodeID() types.StorageNodeID
}
