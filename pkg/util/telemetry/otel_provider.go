package telemetry

import (
	"context"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpgrpc"
	"go.opentelemetry.io/otel/metric/global"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"
)

type openTelmetry struct {
	agentEndpoint  string
	tracerProvider *sdktrace.TracerProvider
	exporter       *otlp.Exporter
	controller     *controller.Controller
}

// var _ Telemetry = (*openTelmetry)(nil)

func newOTELProvider(ctx context.Context, serviceName string, agentEndpoint string) (*openTelmetry, error) {
	driver := otlpgrpc.NewDriver(
		otlpgrpc.WithInsecure(),
		otlpgrpc.WithEndpoint(agentEndpoint),
	)
	exporter, err := otlp.NewExporter(ctx, driver)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(
		exporter,
		sdktrace.WithBatchTimeout(5*time.Second),
		sdktrace.WithMaxExportBatchSize(1024),
	)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithConfig(sdktrace.Config{
			DefaultSampler: sdktrace.TraceIDRatioBased(0.25),
			// sdktrace.AlwaysSample(),

		}),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	cont := controller.New(
		processor.New(
			simple.NewWithExactDistribution(),
			exporter,
		),
		controller.WithPusher(exporter),
		controller.WithCollectPeriod(2*time.Second),
	)

	otel.SetTracerProvider(tracerProvider)
	global.SetMeterProvider(cont.MeterProvider())

	if err := cont.Start(ctx); err != nil {
		return nil, errors.WithStack(err)
	}

	return &openTelmetry{
		agentEndpoint:  agentEndpoint,
		exporter:       exporter,
		tracerProvider: tracerProvider,
	}, nil
}

func (p *openTelmetry) Close(ctx context.Context) error {
	return multierror.Append(p.tracerProvider.Shutdown(ctx), p.controller.Stop(ctx))
}
