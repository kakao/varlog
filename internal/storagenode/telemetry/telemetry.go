package telemetry

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/metric"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/trace"
)

const (
	serviceName = "sn"
)

type TelemetryStub struct {
	tm telemetry.Telemetry
	tr trace.Tracer
	mt metric.Meter
	mb *MetricsBag
}

func newTelemetryStub(ctx context.Context, telemetryType string, storageNodeID types.StorageNodeID, endpoint string) (*TelemetryStub, error) {
	tm, err := telemetry.New(ctx, telemetryType, serviceName, storageNodeID.String(), telemetry.WithEndpoint(endpoint))
	if err != nil {
		return nil, err
	}
	stub := &TelemetryStub{
		tm: tm,
		tr: telemetry.Tracer(telemetry.ServiceNamespace + "." + serviceName),
		mt: telemetry.Meter(telemetry.ServiceNamespace + "." + serviceName),
	}
	stub.mb = newMetricsBag(stub)
	return stub, nil
}

func NewNopTelmetryStub() *TelemetryStub {
	tm := telemetry.NewNopTelemetry()
	stub := &TelemetryStub{
		tm: tm,
		tr: telemetry.Tracer(telemetry.ServiceNamespace + "." + serviceName),
		mt: telemetry.Meter(telemetry.ServiceNamespace + "." + serviceName),
	}
	stub.mb = newMetricsBag(stub)
	return stub
}

func (ts *TelemetryStub) close(ctx context.Context) {
	ts.tm.Close(ctx)
}

func (ts *TelemetryStub) StartSpan(ctx context.Context, name string, opts ...trace.SpanOption) (context.Context, trace.Span) {
	return ts.tr.Start(ctx, name, opts...)
}

func (ts *TelemetryStub) Metrics() *MetricsBag {
	return ts.mb
}
