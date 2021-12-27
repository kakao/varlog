package telemetry

import (
	"context"

	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
)

// StopMeterProvider is the type for stop function of meter provider.
// It stops both meter provider and exporter.
type StopMeterProvider func(context.Context)

// NewMeterProvider creates a new meter provider and its stop function.
func NewMeterProvider(opts ...MeterProviderOption) (metric.MeterProvider, StopMeterProvider, error) {
	cfg := newMeterProviderConfig(opts)

	if cfg.exporter == nil {
		mp := metric.NewNoopMeterProvider()
		return mp, func(context.Context) {}, nil
	}

	pusher := controller.New(
		processor.NewFactory(
			cfg.aggregatorSelector,
			cfg.exporter,
		),
		controller.WithResource(cfg.resource),
		controller.WithExporter(cfg.exporter),
	)

	if cfg.hostInstrumentation {
		if err := initHostInstrumentation(pusher); err != nil {
			return nil, func(context.Context) {}, err
		}
	}

	if cfg.runtimeInstrumentation {
		if err := initRuntimeInstrumentation(pusher, cfg.runtimeInstrumentationOpts); err != nil {
			return nil, func(context.Context) {}, err
		}
	}

	if err := pusher.Start(context.Background()); err != nil {
		return nil, func(context.Context) {}, err
	}

	stop := func(ctx context.Context) {
		if err := pusher.Stop(ctx); err != nil {
			otel.Handle(err)
		}
		cfg.shutdownExporter(ctx)
	}

	return pusher, stop, nil
}

func initHostInstrumentation(mp metric.MeterProvider) error {
	return host.Start(host.WithMeterProvider(mp))
}

func initRuntimeInstrumentation(mp metric.MeterProvider, opts []runtime.Option) error {
	return runtime.Start(append(opts, runtime.WithMeterProvider(mp))...)
}

func SetGlobalMeterProvider(mp metric.MeterProvider) {
	global.SetMeterProvider(mp)
}
