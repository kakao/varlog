package metadata_repository

import (
	"context"
	"errors"
	"sync"

	varlog "github.com/kakao/varlog/pkg/varlog"
	types "github.com/kakao/varlog/pkg/varlog/types"
	snpb "github.com/kakao/varlog/proto/storage_node"
	varlogpb "github.com/kakao/varlog/proto/varlog"
)

type InMemoryMetadataRepository struct {
	metadata        varlogpb.MetadataDescriptor
	globalLogStream []*snpb.GlobalLogStreamDescriptor
	penddingC       chan *snpb.LocalLogStreamDescriptor
	commitC         chan *snpb.GlobalLogStreamDescriptor
	storageMap      map[types.StorageNodeID]varlog.StorageNodeClient
	mu              sync.RWMutex
}

func NewInMemoryMetadataRepository() *InMemoryMetadataRepository {
	r := &InMemoryMetadataRepository{}
	return r
}

func (r *InMemoryMetadataRepository) Close() error {
	return nil
}

func (r *InMemoryMetadataRepository) RegisterStorageNode(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if err := r.metadata.InsertStorageNode(sn); err != nil {
		return varlog.ErrAlreadyExists
	}

	return nil
}

func (r *InMemoryMetadataRepository) CreateLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if err := r.metadata.InsertLogStream(ls); err != nil {
		return varlog.ErrAlreadyExists
	}

	return nil
}

func (r *InMemoryMetadataRepository) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
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
