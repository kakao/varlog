package storagenode

import (
	"go.opentelemetry.io/otel/metric"
)

type metricsBag struct {
	requests metric.Int64UpDownCounter
}

func newMetricsBag(ts *telemetryStub) *metricsBag {
	return &metricsBag{
		requests: metric.Must(ts.mt).NewInt64UpDownCounter("requests"),
	}
}
