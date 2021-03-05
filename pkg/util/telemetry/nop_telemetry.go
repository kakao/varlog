package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type nopTelemetry struct{}

func NewNopTelemetry() *nopTelemetry {
	otel.SetTracerProvider(oteltrace.NewNoopTracerProvider())
	global.SetMeterProvider(otelmetric.NoopMeterProvider{})
	return &nopTelemetry{}
}

func (_ nopTelemetry) Close(_ context.Context) error {
	return nil
}
