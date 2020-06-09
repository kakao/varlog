package metadata_repository

import (
	varlogpb "github.com/kakao/varlog/proto/varlog"
)

type MetadataRepository interface {
	RegisterSequencer(addr string) error
	RegisterStorage(addr, path string, total uint64) error
	UnregisterStorage(addr, path string) error
	UpdateStorage(addr, path string, used uint64) error
	Propose(epoch uint64, projection *varlogpb.ProjectionDescriptor) error
	GetProjection(epoch uint64) (*varlogpb.ProjectionDescriptor, error)
	GetMetadata() (*varlogpb.MetadataDescriptor, error)
}
