package metadata_repository

import (
	"context"

	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type ReporterClientFactory interface {
	GetClient(context.Context, *varlogpb.StorageNodeDescriptor) (storagenode.LogStreamReporterClient, error)
}
