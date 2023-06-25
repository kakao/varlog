package telemetry

import (
	"context"
	"errors"

	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
)

// StopMeterProvider is the type for stop function of meter provider.
// It stops both meter provider and exporter.
type StopMeterProvider func(context.Context) error

// NewMeterProvider creates a new meter provider and its stop function.
func NewMeterProvider(opts ...MeterProviderOption) (metric.MeterProvider, StopMeterProvider, error) {
	cfg := newMeterProviderConfig(opts)

	stop := func(ctx context.Context) error { return nil }

	if cfg.exporter == nil {
		mp := noopmetric.NewMeterProvider()
		return mp, stop, nil
	}

	reader := metricsdk.NewPeriodicReader(cfg.exporter)

	mp := metricsdk.NewMeterProvider(
		metricsdk.WithResource(cfg.resource),
		metricsdk.WithReader(reader),
	)

	if cfg.hostInstrumentation {
		if err := initHostInstrumentation(mp); err != nil {
			return nil, stop, err
		}
	}

	if cfg.runtimeInstrumentation {
		if err := initRuntimeInstrumentation(mp, cfg.runtimeInstrumentationOpts); err != nil {
			return nil, stop, err
		}
	}

	stop = func(ctx context.Context) error {
		return errors.Join(mp.Shutdown(ctx), cfg.exporter.Shutdown(ctx))
	}

	return mp, stop, nil
}

func initHostInstrumentation(mp metric.MeterProvider) error {
	return host.Start(host.WithMeterProvider(mp))
}

func initRuntimeInstrumentation(mp metric.MeterProvider, opts []runtime.Option) error {
	return runtime.Start(append(opts, runtime.WithMeterProvider(mp))...)
}

func SetGlobalMeterProvider(mp metric.MeterProvider) {
	otel.SetMeterProvider(mp)
}

func GetGlobalMeterProvider() metric.MeterProvider {
	return otel.GetMeterProvider()
}
