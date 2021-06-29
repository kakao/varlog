package trace

import (
	oteltrace "go.opentelemetry.io/otel/trace"
)

type (
	Tracer          = oteltrace.Tracer
	Span            = oteltrace.Span
	SpanStartOption = oteltrace.SpanStartOption
)
