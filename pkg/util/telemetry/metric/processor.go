package metric

import (
	otelmetric "go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
)

type (
	Processor = basic.Processor
	Exporter  = otelmetric.Exporter
)

func NewMetricProcessor(exporter Exporter) *Processor {
	aggSelector := simple.NewWithHistogramDistribution()
	// aggSelector := simple.NewWithInexpensiveDistribution()
	return basic.New(aggSelector, exporter)
}
