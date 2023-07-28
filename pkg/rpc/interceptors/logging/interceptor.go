package logging

import (
	"context"
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor returns a new unary server interceptor that logs a
// gRPC error.
func UnaryServerInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
			return handler(ctx, req)
		}
	}

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		start := time.Now()
		resp, err = handler(ctx, req)
		if err != nil {
			duration := time.Since(start)
			logger.Error(info.FullMethod,
				zap.Stringer("code", status.Code(err)),
				zap.Duration("duration", duration),
				zap.Stringer("request", req.(fmt.Stringer)),
				zap.Stringer("response", resp.(fmt.Stringer)),
				zap.String("peer", peerAddr(ctx)),
				zap.Error(err),
			)
		}
		return resp, err
	}
}

func peerAddr(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}

	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return ""
	}

	if host == "" {
		return "127.0.0.1"
	}
	return host
}
