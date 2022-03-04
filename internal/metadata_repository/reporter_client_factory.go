package metadata_repository

import (
	"context"

	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/reportcommitter"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type ReporterClientFactory interface {
	GetReporterClient(context.Context, *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error)
}
