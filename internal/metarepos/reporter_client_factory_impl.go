package metarepos

import (
	"context"

	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/reportcommitter"
	"github.com/kakao/varlog/proto/varlogpb"
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
