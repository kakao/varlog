package metadata_repository

import (
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type MetadataRepository interface {
	Propose(epoch uint64, projection *varlogpb.ProjectionDescriptor) error
	Get(epoch uint64) (*varlogpb.ProjectionDescriptor, error)
}
