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

// Measurable indicates an object that can be measured.
//
// Stub returns a struct Stub.
type Measurable interface {
	Stub() *Stub
}

// Stub is a struct to measure metrics and to trace.
type Stub struct {
	tm telemetry.Telemetry
	tr trace.Tracer
	mt metric.Meter
	mb *MetricsBag
}

func NewTelemetryStub(ctx context.Context, telemetryType string, storageNodeID types.StorageNodeID, endpoint string) (*Stub, error) {
	tm, err := telemetry.New(ctx, serviceName, storageNodeID.String(),
		telemetry.WithExporterType(telemetryType),
		telemetry.WithEndpoint(endpoint),
	)
	if err != nil {
		return nil, err
	}
	stub := &Stub{
		tm: tm,
		tr: telemetry.Tracer(telemetry.ServiceNamespace + "." + serviceName),
		mt: telemetry.Meter(telemetry.ServiceNamespace + "." + serviceName),
	}
	stub.mb = newMetricsBag(stub)
	return stub, nil
}

func NewNopTelmetryStub() *Stub {
	tm := telemetry.NewNopTelemetry()
	stub := &Stub{
		tm: tm,
		tr: telemetry.Tracer(telemetry.ServiceNamespace + "." + serviceName),
		mt: telemetry.Meter(telemetry.ServiceNamespace + "." + serviceName),
	}
	stub.mb = newMetricsBag(stub)
	return stub
}

func (ts *Stub) close(ctx context.Context) {
	ts.tm.Close(ctx)
}

func (ts *Stub) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return ts.tr.Start(ctx, name, opts...)
}

func (ts *Stub) Metrics() *MetricsBag {
	return ts.mb
}

func (ts *Stub) Meter() metric.Meter {
	return ts.mt
}
