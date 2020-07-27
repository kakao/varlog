package metadata_repository

import (
	"context"

	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type MetadataRepository interface {
	RegisterStorageNode(context.Context, *varlogpb.StorageNodeDescriptor) error
	RegisterLogStream(context.Context, *varlogpb.LogStreamDescriptor) error
	UpdateLogStream(context.Context, *varlogpb.LogStreamDescriptor) error
	GetMetadata(context.Context) (*varlogpb.MetadataDescriptor, error)
	Seal(context.Context, types.LogStreamID) (types.GLSN, error)
	Unseal(context.Context, types.LogStreamID) error
	Close() error
}
