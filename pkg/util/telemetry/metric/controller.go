package metric

import (
	otelmetric "go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/metric/controller/basic"
)

const (
	DefaultCollectPeriod  = basic.DefaultPeriod
	DefaultCollectTimeout = basic.DefaultPeriod
	DefaultPushTimeout    = basic.DefaultPeriod
)

type (
	Controller       = basic.Controller
	ControllerConfig = basic.Config
	Checkpointer     = otelmetric.Checkpointer
)

func NewDefaultControllerConfig() *ControllerConfig {
	return &ControllerConfig{
		CollectPeriod:  DefaultCollectPeriod,
		CollectTimeout: DefaultCollectTimeout,
		PushTimeout:    DefaultPushTimeout,
	}
}
func NewController(metricProcessor Checkpointer, exporter Exporter, config *ControllerConfig) *Controller {
	return basic.New(
		metricProcessor,
		basic.WithExporter(exporter),
		basic.WithCollectPeriod(config.CollectPeriod),
		basic.WithCollectTimeout(config.CollectTimeout),
		basic.WithPushTimeout(config.PushTimeout),
		basic.WithResource(config.Resource),
	)
}
