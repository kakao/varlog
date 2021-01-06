package metadata_repository

import (
	"context"

	"github.com/kakao/varlog/pkg/util/telemetry"
	"github.com/kakao/varlog/pkg/util/telemetry/metrics"
	"github.com/kakao/varlog/pkg/util/telemetry/trace"
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
		tr: trace.NamedTracer("varlog.mr"),
		mt: metrics.NamedMeter("varlog.mr"),
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
