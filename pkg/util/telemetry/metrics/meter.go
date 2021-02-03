package metrics

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/controller/push"
	"go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"

	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/exporter"
)

type Meter = metric.Meter

type Closer func()

func New(exporter exporter.Exporter) Closer {
	pusher := push.New(
		basic.New(
			simple.NewWithInexpensiveDistribution(),
			exporter,
		),
		exporter,
	)
	pusher.Start()
	otel.SetMeterProvider(pusher.MeterProvider())
	closer := func() {
		pusher.Stop()
	}
	return closer
}

func NamedMeter(name string) metric.Meter {
	return otel.Meter(name)
}
