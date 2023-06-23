package telemetry

import (
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

type meterProviderConfig struct {
	resource         *resource.Resource
	exporter         metricsdk.Exporter
	shutdownExporter ShutdownExporter

	hostInstrumentation bool

	runtimeInstrumentation     bool
	runtimeInstrumentationOpts []runtime.Option
}

func newMeterProviderConfig(opts []MeterProviderOption) meterProviderConfig {
	cfg := meterProviderConfig{
		resource: resource.Default(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// MeterProviderOption is the interface that applies the value to a meter provider configurations.
type MeterProviderOption func(*meterProviderConfig)

// WithResource sets the Resource of a MeterProvider.
func WithResource(resource *resource.Resource) MeterProviderOption {
	return func(cfg *meterProviderConfig) {
		cfg.resource = resource
	}
}

// WithExporter sets exporter and its shutdown function.
func WithExporter(exporter metricsdk.Exporter, shutdownExporter ShutdownExporter) MeterProviderOption {
	return func(cfg *meterProviderConfig) {
		cfg.exporter = exporter
		cfg.shutdownExporter = shutdownExporter
	}
}

// WithHostInstrumentation enables host instrumentation.
func WithHostInstrumentation() MeterProviderOption {
	return func(cfg *meterProviderConfig) {
		cfg.hostInstrumentation = true
	}
}

// WithRuntimeInstrumentation enables runtime instrumentation.
func WithRuntimeInstrumentation(opts ...runtime.Option) MeterProviderOption {
	return func(cfg *meterProviderConfig) {
		cfg.runtimeInstrumentation = true
		cfg.runtimeInstrumentationOpts = opts
	}
}
