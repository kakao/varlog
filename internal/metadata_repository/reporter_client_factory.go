package metadata_repository

import (
	"context"

	"github.com/kakao/varlog/internal/storagenode_deprecated/reportcommitter"
	"github.com/kakao/varlog/proto/varlogpb"
)

type ReporterClientFactory interface {
	GetReporterClient(context.Context, *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error)
}
