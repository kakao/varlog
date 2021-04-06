package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/metric"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/trace"
)

const ServiceNamespace = "varlog"

type config struct {
	spanProcessorQueueSize    int
	spanProcessorBatchTimeout time.Duration
	spanProcessorMaxBatchSize int
	endpoint                  string
	logger                    *zap.Logger

	metricCollectPeriod  time.Duration
	metricCollectTimeout time.Duration
	metricPushTimeout    time.Duration
}

type Option func(c *config)

type Telemetry interface {
	Close(ctx context.Context) error
}

type basicTelemetry struct {
	traceProvider    *sdktrace.TracerProvider
	metricController *metric.Controller
	exp              exporter
}

func New(ctx context.Context, telemetryType, serviceName, serviceInstanceID string, opts ...Option) (Telemetry, error) {
	if telemetryType == "nop" || telemetryType == "" {
		return NewNopTelemetry(), nil
	}

	cfg := &config{
		spanProcessorQueueSize:    trace.DefaultMaxQueueSize,
		spanProcessorBatchTimeout: trace.DefaultBatchTimeout,
		spanProcessorMaxBatchSize: trace.DefaultBatchSize,
		metricCollectPeriod:       metric.DefaultCollectPeriod,
		metricCollectTimeout:      metric.DefaultCollectTimeout,
		metricPushTimeout:         metric.DefaultPushTimeout,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	// resources
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceNamespaceKey.String(ServiceNamespace),
			semconv.ServiceInstanceIDKey.String(serviceInstanceID),
		),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// exporter
	expCfg := exporterConfig{}
	expCfg.otlpEndpoint = cfg.endpoint
	expCfg.stdoutLogger = cfg.logger
	exp, err := newExporter(ctx, telemetryType, expCfg)
	if err != nil {
		return nil, err
	}

	// span processor
	sp := trace.NewSpanProcessor(
		exp,
		cfg.spanProcessorQueueSize,
		cfg.spanProcessorBatchTimeout,
		cfg.spanProcessorMaxBatchSize,
	)

	// trace provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.3))),
		sdktrace.WithResource(res),
	)

	// metric processor
	mp := metric.NewMetricProcessor(exp)

	// metric controller
	mcCfg := metric.NewDefaultControllerConfig()
	mcCfg.Resource = res
	mcCfg.CollectPeriod = cfg.metricCollectPeriod
	mcCfg.CollectTimeout = cfg.metricCollectTimeout
	mcCfg.PushTimeout = cfg.metricPushTimeout
	mc := metric.NewController(mp, exp, mcCfg)

	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(tp)
	global.SetMeterProvider(mc.MeterProvider())

	// host
	if err := host.Start(host.WithMeterProvider(mc.MeterProvider())); err != nil {
		return nil, errors.WithStack(err)
	}

	// runtime metric
	if err := runtime.Start(
		runtime.WithMinimumReadMemStatsInterval(time.Second),
		runtime.WithMeterProvider(mc.MeterProvider()),
	); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := mc.Start(context.Background()); err != nil {
		return nil, errors.WithStack(err)
	}

	return &basicTelemetry{
		traceProvider:    tp,
		metricController: mc,
		exp:              exp,
	}, nil
}

func (t *basicTelemetry) Close(ctx context.Context) (err error) {
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

func WithSpanProcessorQueueSize(size int) Option {
	return func(c *config) {
		c.spanProcessorQueueSize = size
	}
}

func WithSpanProcessorBatchTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.spanProcessorBatchTimeout = timeout
	}
}

func WithSpanProcessorMaxBatchSize(size int) Option {
	return func(c *config) {
		c.spanProcessorMaxBatchSize = size
	}
}

func WithEndpoint(ep string) Option {
	return func(c *config) {
		c.endpoint = ep
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

func WithMetricCollectPeriod(period time.Duration) Option {
	return func(c *config) {
		c.metricCollectPeriod = period
	}
}

func WithMetricCollectTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.metricCollectTimeout = timeout
	}
}

func WithMetricPushTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.metricPushTimeout = timeout
	}
}

func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

func Meter(name string) metric.Meter {
	return global.Meter(name)
}
