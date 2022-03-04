package metadata_repository

import (
	"context"

	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/reportcommitter"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type reporterClientFactory struct {
	grpcDialOptions []grpc.DialOption
}

func NewReporterClientFactory(grpcDialOptions ...grpc.DialOption) *reporterClientFactory {
	return &reporterClientFactory{
		grpcDialOptions: grpcDialOptions,
	}
}

func (rcf *reporterClientFactory) GetReporterClient(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error) {
	return reportcommitter.NewClient(ctx, sn.Address, rcf.grpcDialOptions...)
}
