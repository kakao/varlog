package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type nopProvider struct{}

var _ Provider = (*nopProvider)(nil)

func newNopProvider() *nopProvider {
	otel.SetTracerProvider(oteltrace.NewNoopTracerProvider())
	otel.SetMeterProvider(otelmetric.NoopMeterProvider{})
	return &nopProvider{}
}

func (np nopProvider) Close(_ context.Context) error {
	return nil
}
