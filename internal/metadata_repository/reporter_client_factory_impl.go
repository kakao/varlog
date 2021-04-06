package metadata_repository

import (
	"context"

	"github.com/kakao/varlog/internal/storagenode/reportcommitter"
	"github.com/kakao/varlog/proto/varlogpb"
)

type reporterClientFactory struct {
}

func NewReporterClientFactory() *reporterClientFactory {
	return &reporterClientFactory{}
}

func (rcf *reporterClientFactory) GetClient(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error) {
	return reportcommitter.NewClient(ctx, sn.Address)
}
