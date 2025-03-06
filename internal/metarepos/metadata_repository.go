package metarepos

import (
	"context"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type MetadataRepository interface {
	RegisterStorageNode(context.Context, *varlogpb.StorageNodeDescriptor) error
	UnregisterStorageNode(context.Context, types.StorageNodeID) error
	RegisterTopic(context.Context, types.TopicID) error
	UnregisterTopic(context.Context, types.TopicID) error
	RegisterLogStream(context.Context, *varlogpb.LogStreamDescriptor) error
	UnregisterLogStream(context.Context, types.LogStreamID) error
	UpdateLogStream(context.Context, *varlogpb.LogStreamDescriptor) error
	GetMetadata(context.Context) (*varlogpb.MetadataDescriptor, error)
	Seal(context.Context, types.LogStreamID) (types.GLSN, error)
	Unseal(context.Context, types.LogStreamID) error
	GetCommitResult(context.Context, types.TopicID, types.LogStreamID) snpb.LogStreamCommitResult
	GetReports(context.Context, types.LogStreamID) mrpb.LogStreamUncommitReports
	Close() error
}
