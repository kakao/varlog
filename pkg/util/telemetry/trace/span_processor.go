package trace

import (
	"time"

	oteltrace "go.opentelemetry.io/otel/sdk/export/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	DefaultBatchTimeout = sdktrace.DefaultBatchTimeout
	DefaultBatchSize    = sdktrace.DefaultMaxExportBatchSize
	DefaultMaxQueueSize = sdktrace.DefaultMaxQueueSize
)

type (
	SpanProcessor = sdktrace.SpanProcessor
	SpanExporter  = oteltrace.SpanExporter
)

func NewSpanProcessor(exporter SpanExporter, queueSize int, batchTimeout time.Duration, maxBatchSize int) SpanProcessor {
	return sdktrace.NewBatchSpanProcessor(
		exporter,
		sdktrace.WithMaxQueueSize(queueSize),
		sdktrace.WithBatchTimeout(DefaultBatchTimeout),
		sdktrace.WithMaxExportBatchSize(DefaultBatchSize),
	)
}
