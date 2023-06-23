package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
)

type ShutdownExporter func(context.Context)

func NewStdoutExporter(opts ...stdoutmetric.Option) (metricsdk.Exporter, ShutdownExporter, error) {
	exp, err := stdoutmetric.New(opts...)
	return exp, func(context.Context) {}, err
}

func NewOLTPExporter(ctx context.Context, clientOpts ...otlpmetricgrpc.Option) (metricsdk.Exporter, ShutdownExporter, error) {
	exp, err := otlpmetricgrpc.New(ctx, clientOpts...)
	if err != nil {
		return nil, func(context.Context) {}, err
	}

	shutdown := func(ctx context.Context) {
		if err := exp.Shutdown(ctx); err != nil {
			otel.Handle(err)
		}
	}

	return exp, shutdown, err
}
