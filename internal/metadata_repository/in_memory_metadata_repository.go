package metadata_repository

import (
	"errors"
	"sync"

	varlog "github.daumkakao.com/varlog/varlog/pkg/varlog"
	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type InMemoryMetadataRepository struct {
	metadata        varlogpb.MetadataDescriptor
	globalLogStream []snpb.GlobalLogStreamDescriptor
	penddingC       chan *snpb.LocalLogStreamDescriptor
	commitC         chan *snpb.GlobalLogStreamDescriptor
	storageMap      map[types.StorageNodeID]varlog.StorageNodeClient
	mu              sync.RWMutex
}

func NewInMemoryMetadataRepository() *InMemoryMetadataRepository {
	r := &InMemoryMetadataRepository{}
	return r
}

func (r *InMemoryMetadataRepository) RegisterStorageNode(sn *varlogpb.StorageNodeDescriptor) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if err := r.metadata.InsertStorageNode(sn); err != nil {
		return varlog.ErrAlreadyExists
	}

	return nil
}

func (r *InMemoryMetadataRepository) CreateLogStream(ls *varlogpb.LogStreamDescriptor) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if err := r.metadata.InsertLogStream(ls); err != nil {
		return varlog.ErrAlreadyExists
	}

	return nil
}

func (r *InMemoryMetadataRepository) GetMetadata() (*varlogpb.MetadataDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &r.metadata, nil
}

func (r *InMemoryMetadataRepository) aggregator() {
	// not yet impliemented
	// call GetReport() to all storage node
}

func (r *InMemoryMetadataRepository) committer() {
	// not yet impliemented
	// calcurate glsn
}

func (r *InMemoryMetadataRepository) deliverer() {
	// not yet impliemented
	// call Commit() to storage node
}

func (r *InMemoryMetadataRepository) penddingReport(report *snpb.LocalLogStreamDescriptor) error {
	r.penddingC <- report
	return nil
}

func (r *InMemoryMetadataRepository) deliveryResult(snId types.StorageNodeID, results []*snpb.GlobalLogStreamDescriptor) error {
	return errors.New("not yet implemented")
}
