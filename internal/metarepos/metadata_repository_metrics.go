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
	gauges   sync.Map
}

func newMetricsBag(mt metric.Meter) *metricsBag {
	counter, err := mt.Int64UpDownCounter("requests")
	if err != nil {
		panic(err)
	}
	return &metricsBag{
		mt:       mt,
		requests: counter,
	}
}

func (mb *metricsBag) Records(name string) metric.Float64Histogram {
	f, ok := mb.records.Load(name)
	if !ok {
		r, err := mb.mt.Float64Histogram(name)
		if err != nil {
			panic(err)
		}
		f, _ = mb.records.LoadOrStore(name, r)
	}

	return f.(metric.Float64Histogram)
}

func (mb *metricsBag) Counts(name string) metric.Int64Counter {
	f, ok := mb.counts.Load(name)
	if !ok {
		r, err := mb.mt.Int64Counter(name)
		if err != nil {
			panic(err)
		}
		f, _ = mb.counts.LoadOrStore(name, r)
	}

	return f.(metric.Int64Counter)
}

func (mb *metricsBag) Gauges(name string) metric.Int64Gauge {
	f, ok := mb.gauges.Load(name)
	if !ok {
		r, err := mb.mt.Int64Gauge(name)
		if err != nil {
			panic(err)
		}
		f, _ = mb.gauges.LoadOrStore(name, r)
	}

	return f.(metric.Int64Gauge)
}
