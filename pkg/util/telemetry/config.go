package telemetry

import (
	"github.com/pkg/errors"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

type config struct {
	bspOpts []tracesdk.BatchSpanProcessorOption
	tpOpts  []tracesdk.TracerProviderOption

	controllerOpts []controller.Option

	endpoint     string
	exporterType string
	logger       *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (c config) validate() error {
	switch c.exporterType {
	case "nop", "":
	case "stdout", "console":
	case "otlp", "otel":
	default:
		return errors.Errorf("invalid exporter type: %s", c.exporterType)
	}
	return nil
}

type Option func(*config)

func WithEndpoint(endpoint string) Option {
	return func(c *config) {
		c.endpoint = endpoint
	}
}

func WithExporterType(epType string) Option {
	return func(c *config) {
		c.exporterType = epType
	}
}

func WithBatchSpanProcessorOptions(bspOpts ...tracesdk.BatchSpanProcessorOption) Option {
	return func(c *config) {
		c.bspOpts = bspOpts
	}
}

func WithTraceProviderOptions(tpOpts ...tracesdk.TracerProviderOption) Option {
	return func(c *config) {
		c.tpOpts = tpOpts
	}
}

func WithMetricControllerOptions(controllerOpts ...controller.Option) Option {
	return func(c *config) {
		c.controllerOpts = controllerOpts
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}
