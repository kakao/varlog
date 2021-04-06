package telemetry

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/unit"
)

type MetricsBag struct {
	ActiveRequests      metric.Int64UpDownCounter
	TotalRequests       metric.Int64Counter
	StorageResponseTime metric.Float64ValueRecorder
}

func newMetricsBag(ts *TelemetryStub) *MetricsBag {
	meter := metric.Must(ts.mt)
	return &MetricsBag{
		ActiveRequests: meter.NewInt64UpDownCounter(
			"sn.active_requests",
			metric.WithDescription("number of active requests"),
			metric.WithUnit(unit.Dimensionless),
		),
		TotalRequests: meter.NewInt64Counter(
			"sn.total_requests",
			metric.WithDescription("number of active requests"),
			metric.WithUnit(unit.Dimensionless),
		),
		StorageResponseTime: meter.NewFloat64ValueRecorder(
			"sn.storage_engine.response_time",
			metric.WithDescription("response time of storage read/write operation in milliseconds"),
			metric.WithUnit(unit.Milliseconds),
		),
	}
}
