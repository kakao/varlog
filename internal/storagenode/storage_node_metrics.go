package storagenode

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/unit"
)

type metricsBag struct {
	activeRequests      metric.Int64UpDownCounter
	totalRequests       metric.Int64Counter
	storageResponseTime metric.Float64ValueRecorder
}

func newMetricsBag(ts *telemetryStub) *metricsBag {
	meter := metric.Must(ts.mt)
	return &metricsBag{
		activeRequests: meter.NewInt64UpDownCounter(
			"sn.active_requests",
			metric.WithDescription("number of active requests"),
			metric.WithUnit(unit.Dimensionless),
		),
		totalRequests: meter.NewInt64Counter(
			"sn.total_requests",
			metric.WithDescription("number of active requests"),
			metric.WithUnit(unit.Dimensionless),
		),
		storageResponseTime: meter.NewFloat64ValueRecorder(
			"sn.storage_engine.response_time",
			metric.WithDescription("response time of storage read/write operation in milliseconds"),
			metric.WithUnit(unit.Milliseconds),
		),
	}
}
