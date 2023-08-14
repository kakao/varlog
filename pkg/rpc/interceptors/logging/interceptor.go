package logging

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/kakao/varlog/pkg/rpc/interceptors"
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
				zap.String("peer", interceptors.PeerAddress(ctx)),
				zap.Error(err),
			)
		}
		return resp, err
	}
}
