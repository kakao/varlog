package metadata_repository

import (
	"context"

	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/proto/varlogpb"
)

type ReporterClientFactory interface {
	GetClient(context.Context, *varlogpb.StorageNodeDescriptor) (storagenode.LogStreamReporterClient, error)
}
