package metadata_repository

import (
	"context"

	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/proto/varlogpb"
)

type reporterClientFactory struct {
}

func NewReporterClientFactory() *reporterClientFactory {
	return &reporterClientFactory{}
}

func (rcf *reporterClientFactory) GetClient(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) (storagenode.LogStreamReporterClient, error) {
	return storagenode.NewLogStreamReporterClient(ctx, sn.Address)
}
