package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
)

func NewStdoutExporter(opts ...stdoutmetric.Option) (metricsdk.Exporter, error) {
	exp, err := stdoutmetric.New(opts...)
	return exp, err
}

func NewOLTPExporter(ctx context.Context, clientOpts ...otlpmetricgrpc.Option) (metricsdk.Exporter, error) {
	exp, err := otlpmetricgrpc.New(ctx, clientOpts...)
	if err != nil {
		return nil, err
	}
	return exp, err
}
