package metadata_repository

import (
	"context"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/reportcommitter"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type reporterClientFactory struct {
}

func NewReporterClientFactory() *reporterClientFactory {
	return &reporterClientFactory{}
}

func (rcf *reporterClientFactory) GetClient(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error) {
	return reportcommitter.NewClient(ctx, sn.Address)
}
