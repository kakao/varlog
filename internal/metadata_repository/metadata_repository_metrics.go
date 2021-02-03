package metadata_repository

import (
	"sync"

	"go.opentelemetry.io/otel/metric"
)

type metricsBag struct {
	mt       metric.Meter
	requests metric.Int64UpDownCounter
	records  sync.Map
}

func newMetricsBag(ts *telemetryStub) *metricsBag {
	return &metricsBag{
		mt:       ts.mt,
		requests: metric.Must(ts.mt).NewInt64UpDownCounter("requests"),
	}
}

func (mb *metricsBag) Records(name string) metric.Float64ValueRecorder {
	f, ok := mb.records.Load(name)
	if !ok {
		r := metric.Must(mb.mt).NewFloat64ValueRecorder(name)
		f, _ = mb.records.LoadOrStore(name, r)
	}

	return f.(metric.Float64ValueRecorder)
}
