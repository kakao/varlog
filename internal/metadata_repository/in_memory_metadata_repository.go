package metadata_repository

import (
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	varlogpb "github.com/kakao/varlog/proto/varlog"
)

type InMemoryMetadataRepository struct {
	metadata varlogpb.MetadataDescriptor
	mu       sync.RWMutex
}

func NewInMemoryMetadataRepository() *InMemoryMetadataRepository {
	r := &InMemoryMetadataRepository{}
	return r
}

func (r *InMemoryMetadataRepository) RegisterSequencer(addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	sequencer := varlogpb.SequencerDescriptor{Address: addr}
	r.metadata.Sequencer = sequencer

	return nil
}

func (r *InMemoryMetadataRepository) RegisterStorage(addr, path string, total uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	storage := varlogpb.StorageDescriptor{
		Path:  path,
		Total: total,
	}

	var psn *varlogpb.StorageNodeDescriptor
	for _, sn := range r.metadata.StorageNodes {
		if sn.Address == addr {
			psn = &sn
		}
	}

	if psn == nil {
		sn := varlogpb.StorageNodeDescriptor{
			StorageNodeId: addr,
			Address:       addr,
		}

		psn = &sn
		r.metadata.StorageNodes = append(r.metadata.StorageNodes, sn)
	}

	psn.Storages = append(psn.Storages, storage)

	return nil
}

func (r *InMemoryMetadataRepository) Propose(epoch uint64, projection *varlogpb.ProjectionDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.metadata.GetProjections()) > 0 && epoch <= r.metadata.GetLastEpoch() {
		return varlog.ErrSealedEpoch
	}

	r.metadata.Projections = append(r.metadata.Projections, *projection)
	return nil
}

func (r *InMemoryMetadataRepository) GetProjection(epoch uint64) (*varlogpb.ProjectionDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.metadata.GetProjection(epoch), nil
}

func (r *InMemoryMetadataRepository) GetMetadata() (*varlogpb.MetadataDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &r.metadata, nil
}

func (r *InMemoryMetadataRepository) UnregisterStorage(addr, path string) error {
	return nil
}

func (r *InMemoryMetadataRepository) UpdateStorage(addr, path string, used uint64) error {
	return nil
}
