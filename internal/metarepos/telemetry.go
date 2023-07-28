package metarepos

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/telemetry"
)

type telemetryStub struct {
	mp   metric.MeterProvider
	stop telemetry.StopMeterProvider
	mb   *metricsBag
}

func newTelemetryStub(ctx context.Context, name string, nodeID types.NodeID, endpoint string) (*telemetryStub, error) {
	// resources
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceName("mr"),
			semconv.ServiceNamespace("varlog"),
			semconv.ServiceInstanceID(nodeID.String()),
		))
	if err != nil {
		return nil, err
	}

	var (
		exporter          metricsdk.Exporter
		shutdown          telemetry.ShutdownExporter
		meterProviderOpts = []telemetry.MeterProviderOption{
			telemetry.WithResource(res),
			telemetry.WithHostInstrumentation(),
			telemetry.WithRuntimeInstrumentation(),
		}
	)

	switch strings.ToLower(name) {
	case "stdout":
		exporter, shutdown, err = telemetry.NewStdoutExporter()
	case "otlp":
		exporter, shutdown, err = telemetry.NewOLTPExporter(ctx, otlpmetricgrpc.WithInsecure(), otlpmetricgrpc.WithEndpoint(endpoint))
	}
	if err != nil {
		return nil, err
	}
	if exporter != nil {
		meterProviderOpts = append(meterProviderOpts, telemetry.WithExporter(exporter, shutdown))
	}

	mp, stop, err := telemetry.NewMeterProvider(meterProviderOpts...)
	if err != nil {
		return nil, err
	}

	telemetry.SetGlobalMeterProvider(mp)

	ts := &telemetryStub{
		mp:   mp,
		stop: stop,
	}

	meter := ts.mp.Meter("varlog.mr")
	ts.mb = newMetricsBag(meter)

	return ts, nil
}

func (ts *telemetryStub) close(ctx context.Context) {
	ts.stop(ctx)
}

func newNopTelmetryStub() *telemetryStub {
	ts, _ := newTelemetryStub(context.Background(), "nop", types.InvalidNodeID, "")
	return ts
}
