package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/propagation"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"

	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/instrumentation/host"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/instrumentation/runtime"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/metric"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/trace"
)

const ServiceNamespace = "varlog"

func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

func Meter(name string) metric.Meter {
	return global.Meter(name)
}

type Telemetry interface {
	Close(ctx context.Context) error
}

type telemetryImpl struct {
	config
	traceProvider    *tracesdk.TracerProvider
	metricController *controller.Controller
	exp              *exporter
}

var _ Telemetry = (*telemetryImpl)(nil)

func New(ctx context.Context, serviceName, serviceInstanceID string, opts ...Option) (Telemetry, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	if cfg.exporterType == "nop" || cfg.exporterType == "" {
		return NewNopTelemetry(), nil
	}

	// resources
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithHost(),
		// resource.WithProcessExecutableName(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceNamespaceKey.String(ServiceNamespace),
			semconv.ServiceInstanceIDKey.String(serviceInstanceID),
		))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// exporter
	exp, err := newExporter(ctx, cfg.exporterType, exporterConfig{
		otlpEndpoint: cfg.endpoint,
		stdoutLogger: cfg.logger,
	})
	if err != nil {
		return nil, err
	}

	// span processor
	sp := tracesdk.NewBatchSpanProcessor(exp.traceExporter, cfg.bspOpts...)

	// trace provider
	tp := tracesdk.NewTracerProvider(append(
		cfg.tpOpts,
		tracesdk.WithSpanProcessor(sp),
		tracesdk.WithSampler(tracesdk.ParentBased(tracesdk.TraceIDRatioBased(0.3))),
		tracesdk.WithResource(res),
	)...)

	// metric controller
	mc := controller.New(
		processor.New(
			simple.NewWithHistogramDistribution(),
			exp.metricExporter,
		),
		controller.WithResource(res),
		controller.WithExporter(exp.metricExporter),
	)

	otel.SetTracerProvider(tp)
	global.SetMeterProvider(mc.MeterProvider())
	propagator := propagation.NewCompositeTextMapPropagator(propagation.Baggage{}, propagation.TraceContext{})
	otel.SetTextMapPropagator(propagator)

	// host
	if err := host.Start(host.WithMeterProvider(mc.MeterProvider())); err != nil {
		return nil, errors.WithStack(err)
	}

	// runtime metric
	if err := runtime.Start(
		// FIXME: make it configurable
		runtime.WithMinimumReadMemStatsInterval(10*time.Second),
		runtime.WithMeterProvider(mc.MeterProvider()),
	); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := mc.Start(context.Background()); err != nil {
		return nil, errors.WithStack(err)
	}

	return &telemetryImpl{
		traceProvider:    tp,
		metricController: mc,
		exp:              exp,
	}, nil
}

func (t *telemetryImpl) Close(ctx context.Context) (err error) {
	errc := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		errc <- t.traceProvider.Shutdown(ctx)
	}()
	go func() {
		defer wg.Done()
		errc <- t.metricController.Stop(ctx)
	}()
	wg.Wait()
	close(errc)
	for e := range errc {
		err = multierr.Append(err, e)
	}
	return err
}

type nopTelemetry struct{}

var _ Telemetry = (*nopTelemetry)(nil)

func NewNopTelemetry() *nopTelemetry {
	otel.SetTracerProvider(oteltrace.NewNoopTracerProvider())
	global.SetMeterProvider(otelmetric.NoopMeterProvider{})
	return &nopTelemetry{}
}

func (_ nopTelemetry) Close(_ context.Context) error {
	return nil
}
