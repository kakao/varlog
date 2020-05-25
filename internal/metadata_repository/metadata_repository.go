package metadata_repository

import "github.daumkakao.com/wokl/solar/pkg/solar"

type MetadataRepository interface {
	Propose(projection *solar.Projection) error
	GetProjection() (*solar.Projection, error)
}
