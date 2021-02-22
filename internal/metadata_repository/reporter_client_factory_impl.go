package metadata_repository

import (
	"context"

	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type reporterClientFactory struct {
}

func NewReporterClientFactory() *reporterClientFactory {
	return &reporterClientFactory{}
}

func (rcf *reporterClientFactory) GetClient(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) (storagenode.LogStreamReporterClient, error) {
	return storagenode.NewLogStreamReporterClient(ctx, sn.Address)
}
