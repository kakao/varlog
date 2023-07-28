package metarepos

import (
	"context"

	"go.opentelemetry.io/otel/metric"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/telemetry"
)

type telemetryStub struct {
	mp metric.MeterProvider
	mb *metricsBag
}

func newTelemetryStub(ctx context.Context, name string, nodeID types.NodeID, endpoint string) (*telemetryStub, error) {
	mp := telemetry.GetGlobalMeterProvider()

	ts := &telemetryStub{
		mp: mp,
	}

	meter := ts.mp.Meter("varlog.mr")
	ts.mb = newMetricsBag(meter)

	return ts, nil
}

func newNopTelmetryStub() *telemetryStub {
	ts, _ := newTelemetryStub(context.Background(), "nop", types.InvalidNodeID, "")
	return ts
}
