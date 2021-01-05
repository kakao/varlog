package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/exporters/trace/jaeger"
	"go.opentelemetry.io/otel/label"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/exporter"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/metrics"
)

type simpleProvider struct {
	flush         func()
	metricsCloser metrics.Closer
}

var _ Provider = (*simpleProvider)(nil)

func newSimpleProvider(serviceName string, tracerTags []label.KeyValue, collectorEndpoint string) (*simpleProvider, error) {
	exporter, err := exporter.NewStdoutExporter()
	if err != nil {
		return nil, err
	}
	metricsCloser := metrics.New(exporter)

	// Create and install Jaeger export pipeline.
	flush, err := jaeger.InstallNewPipeline(
		jaeger.WithCollectorEndpoint(collectorEndpoint),
		jaeger.WithProcess(jaeger.Process{
			ServiceName: serviceName,
			Tags:        tracerTags,
		}),
		jaeger.WithSDK(&sdktrace.Config{DefaultSampler: sdktrace.AlwaysSample()}),
	)
	if err != nil {
		return nil, err
	}
	return &simpleProvider{
		flush:         flush,
		metricsCloser: metricsCloser,
	}, nil
}

func (sp *simpleProvider) Close(_ context.Context) error {
	sp.flush()
	sp.metricsCloser()
	return nil
}
