package metadata_repository

import (
	"sync"

	"github.daumkakao.com/solar/solar/pkg/solar"
	solarpb "github.daumkakao.com/solar/solar/proto/solar"
)

type InMemoryMetadataRepository struct {
	solarpb.ProjectionDescriptor
	mu sync.RWMutex
}

func NewInMemoryMetadataRepository() *InMemoryMetadataRepository {
	r := &InMemoryMetadataRepository{}
	r.Epoch = 0
	return r
}

func (r *InMemoryMetadataRepository) Propose(epoch uint64, projection *solarpb.ProjectionDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if epoch < r.Epoch {
		return solar.ErrSealedEpoch
	}
	r.ProjectionDescriptor = *projection
	return nil
}

func (r *InMemoryMetadataRepository) Get() (*solarpb.ProjectionDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return &r.ProjectionDescriptor, nil
}
