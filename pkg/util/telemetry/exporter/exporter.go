package exporter

import (
	"go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/export/trace"
)

type Exporter interface {
	metric.Exporter
	trace.SpanExporter
}
