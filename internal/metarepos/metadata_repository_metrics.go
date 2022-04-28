package metarepos

import (
	"sync"

	"go.opentelemetry.io/otel/metric"
)

type metricsBag struct {
	mt       metric.Meter
	requests metric.Int64UpDownCounter
	records  sync.Map
	counts   sync.Map
}

func newMetricsBag(mt metric.Meter) *metricsBag {
	return &metricsBag{
		mt:       mt,
		requests: metric.Must(mt).NewInt64UpDownCounter("requests"),
	}
}

func (mb *metricsBag) Records(name string) metric.Float64Histogram {
	f, ok := mb.records.Load(name)
	if !ok {
		r := metric.Must(mb.mt).NewFloat64Histogram(name)
		f, _ = mb.records.LoadOrStore(name, r)
	}

	return f.(metric.Float64Histogram)
}

func (mb *metricsBag) Counts(name string) metric.Int64Counter {
	f, ok := mb.counts.Load(name)
	if !ok {
		r := metric.Must(mb.mt).NewInt64Counter(name)
		f, _ = mb.records.LoadOrStore(name, r)
	}

	return f.(metric.Int64Counter)
}
