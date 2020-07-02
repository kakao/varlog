package metadata_repository

import (
	"context"

	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type MetadataRepository interface {
	RegisterStorageNode(context.Context, *varlogpb.StorageNodeDescriptor) error
	CreateLogStream(context.Context, *varlogpb.LogStreamDescriptor) error
	GetMetadata(context.Context) (*varlogpb.MetadataDescriptor, error)
	Close() error
}
