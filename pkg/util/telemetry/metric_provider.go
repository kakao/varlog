package telemetry

import (
	"context"
	"errors"
	"os"
	"slices"
	"time"

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

	mpOpts := []metricsdk.Option{
		metricsdk.WithResource(cfg.resource),
		metricsdk.WithReader(reader),
		metricsdk.WithView(metricsdk.NewView(
			// It customizes the bucket size of the rpc.server.duration.
			metricsdk.Instrument{
				Name: "rpc.server.duration",
			},
			metricsdk.Stream{
				Aggregation: metricsdk.AggregationExplicitBucketHistogram{
					// 50us, 100us,
					// 1_000us(1ms), 2_000us(2ms), 4_000us(4ms),
					// 8_000us(8ms), 16_000us(16ms), 32_000us(32ms),
					// 1_000_000us(1s), 5_000_000us(5s), 10_000_000us(10s),
					Boundaries: []float64{50, 100, 1e3, 2e3, 4e3, 8e3, 16e3, 32e3, 1e6, 5e6, 1e7},
				},
			},
		)),
	}
	if cfg.runtimeInstrumentation {
		// It customizes the bucket size of the process.runtime.go.gc.pause_ns.
		var boundaries []float64
		// 50us, 100us, 150us, ..., 1ms
		for dur := 50 * time.Microsecond; dur <= time.Millisecond; dur += 50 * time.Microsecond {
			boundaries = append(boundaries, float64(dur))
		}
		// 5ms, 10ms, 15ms, 20ms, 25ms, 30ms, ..., 95ms
		for dur := 5 * time.Millisecond; dur < 100*time.Millisecond; dur += 5 * time.Millisecond {
			boundaries = append(boundaries, float64(dur))
		}
		// 100ms, 200ms, 300ms, ..., 1000ms
		for dur := 100 * time.Millisecond; dur <= 1000*time.Millisecond; dur += 100 * time.Millisecond {
			boundaries = append(boundaries, float64(dur))
		}
		mpOpts = append(mpOpts,
			metricsdk.WithView(metricsdk.NewView(
				metricsdk.Instrument{
					Name: "process.runtime.go.gc.pause_ns",
				},
				metricsdk.Stream{
					Aggregation: metricsdk.AggregationExplicitBucketHistogram{
						Boundaries: boundaries,
					},
				},
			)),
		)
	}

	mp := metricsdk.NewMeterProvider(mpOpts...)

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
	const deprecatedRuntimeFeature = "OTEL_GO_X_DEPRECATED_RUNTIME_METRICS"
	old := os.Getenv(deprecatedRuntimeFeature)
	defer func() {
		_ = os.Setenv(deprecatedRuntimeFeature, old)
	}()

	opts = slices.Concat(opts, []runtime.Option{runtime.WithMeterProvider(mp)})

	_ = os.Setenv(deprecatedRuntimeFeature, "false")
	if err := runtime.Start(slices.Clone(opts)...); err != nil {
		return err
	}

	_ = os.Setenv(deprecatedRuntimeFeature, "true")
	return runtime.Start(slices.Clone(opts)...)
}

func SetGlobalMeterProvider(mp metric.MeterProvider) {
	otel.SetMeterProvider(mp)
}

func GetGlobalMeterProvider() metric.MeterProvider {
	return otel.GetMeterProvider()
}
