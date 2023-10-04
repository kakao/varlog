package otelgrpc

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kakao/varlog/pkg/rpc/interceptors"
)

const (
	instrumentationName = "github.com/kakao/varlog/pkg/rpc/interceptors/otelgrpc"
)

// UnaryServerInterceptor returns a new unary server interceptor that records
// OpenTelemetry metrics for gRPC.
// It follows [OTel 1.22.0](https://opentelemetry.io/docs/specs/otel/metrics/semantic_conventions/rpc-metrics/)
// specification except for the unit of rpc.server.duration, which converts
// from milliseconds to microseconds for high resolution.
func UnaryServerInterceptor(meterProvider metric.MeterProvider) grpc.UnaryServerInterceptor {
	meter := meterProvider.Meter(
		instrumentationName,
		metric.WithSchemaURL(semconv.SchemaURL),
	)
	rpcServerDuration, err := meter.Int64Histogram(
		"rpc.server.duration",
		metric.WithDescription("measures duration of inbound RPC in microseconds"),
		metric.WithUnit("us"),
	)
	if err != nil {
		otel.Handle(err)
	}

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		code := codes.OK

		defer func(start time.Time) {
			elapsedTime := time.Since(start) / time.Microsecond

			attrs := make([]attribute.KeyValue, 0, 4)
			attrs = append(attrs, semconv.RPCSystemGRPC, semconv.RPCGRPCStatusCodeKey.Int64(int64(code)))
			service, method := interceptors.ParseFullMethod(info.FullMethod)
			if service != "" {
				attrs = append(attrs, semconv.RPCServiceKey.String(service))
			}
			if method != "" {
				attrs = append(attrs, semconv.RPCMethodKey.String(method))
			}

			rpcServerDuration.Record(ctx, int64(elapsedTime), metric.WithAttributes(attrs...))
		}(time.Now())

		resp, err = handler(ctx, req)
		if err != nil {
			code = status.Code(err)
		}
		return resp, err
	}
}
