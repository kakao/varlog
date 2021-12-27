package telemetry

import (
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	metricsdk "go.opentelemetry.io/otel/sdk/export/metric"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"go.opentelemetry.io/otel/sdk/resource"
)

type meterProviderConfig struct {
	resource           *resource.Resource
	collectPeriod      time.Duration
	collectTimeout     time.Duration
	pushTimeout        time.Duration
	exporter           metricsdk.Exporter
	shutdownExporter   ShutdownExporter
	aggregatorSelector metricsdk.AggregatorSelector

	hostInstrumentation bool

	runtimeInstrumentation     bool
	runtimeInstrumentationOpts []runtime.Option
}

func newMeterProviderConfig(opts []MeterProviderOption) meterProviderConfig {
	cfg := meterProviderConfig{
		resource:           resource.Default(),
		collectPeriod:      controller.DefaultPeriod,
		collectTimeout:     controller.DefaultPeriod,
		pushTimeout:        controller.DefaultPeriod,
		aggregatorSelector: simple.NewWithInexpensiveDistribution(),
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

// WithCollectPeriod sets CollectPeriod of a MeterProvider.
//
// CollectPeriod is the interval between calls to Collect a checkpoint.
func WithCollectPeriod(collectPeriod time.Duration) MeterProviderOption {
	return func(cfg *meterProviderConfig) {
		cfg.collectPeriod = collectPeriod
	}
}

// WithCollectTimeout sets CollectTimeout of a MeterProvider.
//
// CollectTimeout is the timeout of the Context passed to Collect() and subsequently to Observer
// instrument callbacks.
func WithCollectTimeout(collectTimeout time.Duration) MeterProviderOption {
	return func(cfg *meterProviderConfig) {
		cfg.collectTimeout = collectTimeout
	}
}

// WithPushTimeout sets PushTimeout of a MeterProvider.
//
// PushTimeout is the timeout of the Context when a exporter is configured.
func WithPushTimeout(pushTimeout time.Duration) MeterProviderOption {
	return func(cfg *meterProviderConfig) {
		cfg.pushTimeout = pushTimeout
	}
}

// WithExporter sets exporter and its shutdown function.
func WithExporter(exporter metricsdk.Exporter, shutdownExporter ShutdownExporter) MeterProviderOption {
	return func(cfg *meterProviderConfig) {
		cfg.exporter = exporter
		cfg.shutdownExporter = shutdownExporter
	}
}

// WithAggregatorSelector sets selector of aggregator.
func WithAggregatorSelector(aggregatorSelector metricsdk.AggregatorSelector) MeterProviderOption {
	return func(cfg *meterProviderConfig) {
		cfg.aggregatorSelector = aggregatorSelector
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
