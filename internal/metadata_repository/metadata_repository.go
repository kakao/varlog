package metadata_repository

import (
	solarpb "github.daumkakao.com/solar/solar/proto/solar"
)

type MetadataRepository interface {
	Propose(epoch uint64, projection *solarpb.ProjectionDescriptor) error
	Get() (*solarpb.ProjectionDescriptor, error)
}
