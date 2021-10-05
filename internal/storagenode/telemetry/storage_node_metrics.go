package telemetry

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/unit"
)

type MetricsBag struct {
	RPCServerAppendDuration    metric.Float64Histogram
	RPCServerReplicateDuration metric.Float64Histogram

	ExecutorWriteQueueTime  metric.Float64Histogram
	ExecutorWriteQueueTasks metric.Int64Histogram
	ExecutorWriteTime       metric.Float64Histogram

	ExecutorCommitQueueTime  metric.Float64Histogram
	ExecutorCommitQueueTasks metric.Int64Histogram
	ExecutorCommitTime       metric.Float64Histogram

	ExecutorCommitWaitQueueTime  metric.Float64Histogram
	ExecutorCommitWaitQueueTasks metric.Int64Histogram

	ExecutorReplicateQueueTime metric.Float64Histogram
	ExecutorReplicateTime      metric.Float64Histogram
	// ExecutorReplicateRequestPropagationTime  metric.Float64Histogram
	// ExecutorReplicateResponsePropagationTime metric.Float64Histogram

	ExecutorReplicateConnectionGetTime      metric.Float64Histogram
	ExecutorReplicateRequestPrepareTime     metric.Float64Histogram
	ExecutorReplicateClientRequestQueueTime metric.Float64Histogram
	ExecutorReplicateFanoutTime             metric.Float64Histogram
}

func newMetricsBag(ts *Stub) *MetricsBag {
	meter := metric.Must(ts.mt)
	return &MetricsBag{
		RPCServerAppendDuration: meter.NewFloat64Histogram(
			"rpc.server.append.duration",
			metric.WithUnit(unit.Milliseconds),
		),
		RPCServerReplicateDuration: meter.NewFloat64Histogram(
			"rpc.server.replicate.duration",
			metric.WithUnit(unit.Milliseconds),
		),

		ExecutorWriteQueueTime: meter.NewFloat64Histogram(
			"executor.write_queue.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorWriteQueueTasks: meter.NewInt64Histogram(
			"executor.write_queue.tasks",
			metric.WithUnit(unit.Dimensionless),
		),
		ExecutorWriteTime: meter.NewFloat64Histogram(
			"executor.write.time",
			metric.WithUnit(unit.Milliseconds),
		),

		ExecutorCommitQueueTime: meter.NewFloat64Histogram(
			"executor.commit_queue.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorCommitQueueTasks: meter.NewInt64Histogram(
			"executor.commit_queue.tasks",
			metric.WithUnit(unit.Dimensionless),
		),
		ExecutorCommitTime: meter.NewFloat64Histogram(
			"executor.commit.time",
			metric.WithUnit(unit.Milliseconds),
		),

		ExecutorCommitWaitQueueTime: meter.NewFloat64Histogram(
			"executor.commit_wait_queue.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorCommitWaitQueueTasks: meter.NewInt64Histogram(
			"executor.commit_wait_queue.tasks",
			metric.WithUnit(unit.Dimensionless),
		),

		ExecutorReplicateQueueTime: meter.NewFloat64Histogram(
			"executor.replicate_queue.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorReplicateTime: meter.NewFloat64Histogram(
			"executor.replicate.time",
			metric.WithUnit(unit.Milliseconds),
		),
		/*
			ExecutorReplicateRequestPropagationTime: meter.NewFloat64Histogram(
				"sn.executor.replicate.request.propagation.time",
				metric.WithUnit(unit.Milliseconds),
			),
			ExecutorReplicateResponsePropagationTime: meter.NewFloat64Histogram(
				"sn.executor.replicate.response.propagation.time",
				metric.WithUnit(unit.Milliseconds),
			),
		*/
		ExecutorReplicateConnectionGetTime: meter.NewFloat64Histogram(
			"sn.executor.replicate.connection.get.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorReplicateRequestPrepareTime: meter.NewFloat64Histogram(
			"sn.executor.replicate.request.prepare.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorReplicateClientRequestQueueTime: meter.NewFloat64Histogram(
			"sn.executor.replicate.client.request_queue.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorReplicateFanoutTime: meter.NewFloat64Histogram(
			"sn.executor.replicate.fanout.time",
			metric.WithUnit(unit.Milliseconds),
		),
	}
}
