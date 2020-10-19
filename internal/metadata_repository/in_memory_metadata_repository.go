package metadata_repository

import (
	"context"
	"errors"
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type InMemoryMetadataRepository struct {
	metadata        varlogpb.MetadataDescriptor
	globalLogStream []*snpb.GlobalLogStreamDescriptor
	penddingC       chan *snpb.LocalLogStreamDescriptor
	commitC         chan *snpb.GlobalLogStreamDescriptor
	storageMap      map[types.StorageNodeID]varlog.LogIOClient
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
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.metadata.InsertStorageNode(sn); err != nil {
		return varlog.ErrAlreadyExists
	}

	return nil
}

func (r *InMemoryMetadataRepository) UnregisterStorageNode(ctx context.Context, snID types.StorageNodeID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.metadata.DeleteStorageNode(snID)

	return nil
}

func (r *InMemoryMetadataRepository) RegisterLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.metadata.InsertLogStream(ls); err != nil {
		return varlog.ErrAlreadyExists
	}

	return nil
}

func (r *InMemoryMetadataRepository) UnregisterLogStream(ctx context.Context, lsID types.LogStreamID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.metadata.DeleteLogStream(lsID)

	return nil
}

func (r *InMemoryMetadataRepository) UpdateLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.metadata.UpdateLogStream(ls); err != nil {
		return varlog.ErrNotExist
	}

	return nil
}

func (r *InMemoryMetadataRepository) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &r.metadata, nil
}

func (r *InMemoryMetadataRepository) Seal(ctx context.Context, lsID types.LogStreamID) (types.GLSN, error) {
	return types.GLSN(0), errors.New("not yet implemented")
}

func (r *InMemoryMetadataRepository) Unseal(ctx context.Context, lsID types.LogStreamID) error {
	return errors.New("not yet implemented")
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
