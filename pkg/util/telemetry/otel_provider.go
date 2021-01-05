package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/util/telemetry/exporter"
	"github.com/kakao/varlog/pkg/util/telemetry/metrics"
)

type otelProvider struct {
	agentEndpoint  string
	tracerProvider *sdktrace.TracerProvider
	metricCloser   metrics.Closer
	exporter       exporter.Exporter
}

var _ Provider = (*otelProvider)(nil)

func newOTELProvider(ctx context.Context, serviceName string, agentEndpoint string) (*otelProvider, error) {
	exporter, err := otlp.NewExporter(
		ctx,
		otlp.WithInsecure(),
		otlp.WithAddress(agentEndpoint),
		otlp.WithGRPCDialOption(grpc.WithBlock()), // useful for testing
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceNameKey.String(serviceName),
		),
	)

	bsp := sdktrace.NewBatchSpanProcessor(exporter, sdktrace.WithBatchTimeout(5), sdktrace.WithMaxExportBatchSize(10))
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithConfig(sdktrace.Config{DefaultSampler: sdktrace.AlwaysSample()}),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	metricCloser := metrics.New(exporter)

	otel.SetTracerProvider(tracerProvider)

	return &otelProvider{
		agentEndpoint:  agentEndpoint,
		exporter:       exporter,
		tracerProvider: tracerProvider,
		metricCloser:   metricCloser,
	}, nil
}

func (p *otelProvider) Close(ctx context.Context) (err error) {
	if errTp := p.tracerProvider.Shutdown(ctx); errTp != nil {
		err = errTp
	}
	p.metricCloser()
	if errExp := p.exporter.Shutdown(ctx); errExp != nil {
		err = errExp
	}
	return err
}
