package rpc

import (
	"math"
	"slices"

	"google.golang.org/grpc"
)

// NewServer calls grpc.NewServer function. The package
// github.com/kakao/varlog/pkg/rpc registers the gogoproto codec to the gRPC.
// Therefore calling this method rather than grpc.NewServer makes the
// application server use the gogoproto codec instead of the regular proto
// codec.
func NewServer(opts ...grpc.ServerOption) *grpc.Server {
	defaultServerOptions := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
	}
	opts = slices.Concat(defaultServerOptions, opts)
	return grpc.NewServer(opts...)
}
