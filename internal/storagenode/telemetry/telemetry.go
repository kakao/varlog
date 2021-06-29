package telemetry

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/telemetry -package telemetry -destination telemetry_mock.go . Measurable

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

type Measurable interface {
	Stub() *TelemetryStub
}

type TelemetryStub struct {
	tm telemetry.Telemetry
	tr trace.Tracer
	mt metric.Meter
	mb *MetricsBag
}

func NewTelemetryStub(ctx context.Context, telemetryType string, storageNodeID types.StorageNodeID, endpoint string) (*TelemetryStub, error) {
	tm, err := telemetry.New(ctx, serviceName, storageNodeID.String(),
		telemetry.WithExporterType(telemetryType),
		telemetry.WithEndpoint(endpoint),
	)
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

func (ts *TelemetryStub) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return ts.tr.Start(ctx, name, opts...)
}

func (ts *TelemetryStub) Metrics() *MetricsBag {
	return ts.mb
}

func (ts *TelemetryStub) Meter() metric.Meter {
	return ts.mt
}
