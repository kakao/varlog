package metadata_repository

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/metric"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/trace"
)

const serviceName = "mr"

type telemetryStub struct {
	tm telemetry.Telemetry
	tr trace.Tracer
	mt metric.Meter
	mb *metricsBag
}

func newTelemetryStub(ctx context.Context, name string, nodeID types.NodeID, endpoint string) (*telemetryStub, error) {
	tm, err := telemetry.New(ctx, serviceName, nodeID.String(),
		telemetry.WithExporterType(name),
		telemetry.WithEndpoint(endpoint))
	if err != nil {
		return nil, err
	}
	stub := &telemetryStub{
		tm: tm,
		tr: telemetry.Tracer(telemetry.ServiceNamespace + "." + serviceName),
		mt: telemetry.Meter(telemetry.ServiceNamespace + "." + serviceName),
	}
	stub.mb = newMetricsBag(stub)
	return stub, nil
}

func newNopTelmetryStub() *telemetryStub {
	tm := telemetry.NewNopTelemetry()
	stub := &telemetryStub{
		tm: tm,
		tr: telemetry.Tracer(telemetry.ServiceNamespace + "." + serviceName),
		mt: telemetry.Meter(telemetry.ServiceNamespace + "." + serviceName),
	}
	stub.mb = newMetricsBag(stub)
	return stub
}

func (ts *telemetryStub) close(ctx context.Context) {
	ts.tm.Close(ctx)
}

func (ts *telemetryStub) startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return ts.tr.Start(ctx, name, opts...)
}

func (ts *telemetryStub) metrics() *metricsBag {
	return ts.mb
}
