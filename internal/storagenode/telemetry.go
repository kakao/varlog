package storagenode

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/metrics"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/trace"
)

type telemetryStub struct {
	tm *telemetry.Telemetry
	tr trace.Tracer
	mt metrics.Meter
	mb *metricsBag
}

func newTelemetryStub(name string, endpoint string) *telemetryStub {
	tm, err := telemetry.New(name, endpoint)
	if err != nil {
		panic(err)
	}
	stub := &telemetryStub{
		tm: tm,
		tr: trace.NamedTracer("varlog.storagenode"),
		mt: metrics.NamedMeter("varlog.storagenode"),
	}
	stub.mb = newMetricsBag(stub)
	return stub
}

func newNopTelmetryStub() *telemetryStub {
	return newTelemetryStub("nop", "")
}

func (ts *telemetryStub) close(ctx context.Context) {
	ts.tm.Close(ctx)
}

func (ts *telemetryStub) startSpan(ctx context.Context, name string, opts ...trace.SpanOption) (context.Context, trace.Span) {
	return ts.tr.Start(ctx, name, opts...)
}

func (ts *telemetryStub) metrics() *metricsBag {
	return ts.mb
}
