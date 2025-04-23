package rpc

import (
	"math"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	// networkTimeout defines the timeout for network operations. Since Varlog
	// is designed for deployment in Ethernet networks, we set it to 500ms
	// based on the expected upper bound of ping times (100ms) and 2 x
	// TCP_RTO_MIN (200ms). This aggressive setting ensures responsiveness to
	// network changes.
	//
	// For a more conservative approach supporting multi-region deployments,
	// refer to:
	// https://github.com/cockroachdb/cockroach/blob/70bb4fafea5534d34fab9440975da5526933c955/pkg/base/config.go#L120-L137
	networkTimeout = 500 * time.Millisecond

	// serverKeepaliveInterval defines the interval between server-side
	// keepalive pings. We set this to 2000ms (4*networkTimeout) to ensure that
	// keepalive pings are sent only after waiting long enough for regular
	// network activity (e.g., RPC heartbeats) to occur, reducing unnecessary
	// pings while maintaining connection reliability.
	serverKeepaliveInterval = 4 * networkTimeout

	// serverKeepaliveTimeout specifies the timeout for server-side keepalive
	// pings. If a response to a keepalive ping is not received within this
	// duration, the connection is considered failed and will be closed. We set
	// this to 1500ms (3*networkTimeout) to allow sufficient time for transient
	// network issues to resolve, ensuring robustness in detecting failed
	// connections without prematurely closing them.
	serverKeepaliveTimeout = 3 * networkTimeout
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
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    serverKeepaliveInterval,
			Timeout: serverKeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Millisecond,
			PermitWithoutStream: true,
		}),
	}
	opts = slices.Concat(defaultServerOptions, opts)
	return grpc.NewServer(opts...)
}
