package telemetry

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/unit"
)

type MetricsBag struct {
	RPCServerAppendDuration    metric.Float64ValueRecorder
	RPCServerReplicateDuration metric.Float64ValueRecorder

	ExecutorWriteQueueTime  metric.Float64ValueRecorder
	ExecutorWriteQueueTasks metric.Int64ValueRecorder
	ExecutorWriteTime       metric.Float64ValueRecorder

	ExecutorCommitQueueTime  metric.Float64ValueRecorder
	ExecutorCommitQueueTasks metric.Int64ValueRecorder
	ExecutorCommitTime       metric.Float64ValueRecorder

	ExecutorCommitWaitQueueTime  metric.Float64ValueRecorder
	ExecutorCommitWaitQueueTasks metric.Int64ValueRecorder

	ExecutorReplicateQueueTime metric.Float64ValueRecorder
	ExecutorReplicateTime      metric.Float64ValueRecorder
	// ExecutorReplicateRequestPropagationTime  metric.Float64ValueRecorder
	// ExecutorReplicateResponsePropagationTime metric.Float64ValueRecorder

	ExecutorReplicateConnectionGetTime      metric.Float64ValueRecorder
	ExecutorReplicateRequestPrepareTime     metric.Float64ValueRecorder
	ExecutorReplicateClientRequestQueueTime metric.Float64ValueRecorder
	ExecutorReplicateFanoutTime             metric.Float64ValueRecorder
}

func newMetricsBag(ts *Stub) *MetricsBag {
	meter := metric.Must(ts.mt)
	return &MetricsBag{
		RPCServerAppendDuration: meter.NewFloat64ValueRecorder(
			"rpc.server.append.duration",
			metric.WithUnit(unit.Milliseconds),
		),
		RPCServerReplicateDuration: meter.NewFloat64ValueRecorder(
			"rpc.server.replicate.duration",
			metric.WithUnit(unit.Milliseconds),
		),

		ExecutorWriteQueueTime: meter.NewFloat64ValueRecorder(
			"executor.write_queue.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorWriteQueueTasks: meter.NewInt64ValueRecorder(
			"executor.write_queue.tasks",
			metric.WithUnit(unit.Dimensionless),
		),
		ExecutorWriteTime: meter.NewFloat64ValueRecorder(
			"executor.write.time",
			metric.WithUnit(unit.Milliseconds),
		),

		ExecutorCommitQueueTime: meter.NewFloat64ValueRecorder(
			"executor.commit_queue.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorCommitQueueTasks: meter.NewInt64ValueRecorder(
			"executor.commit_queue.tasks",
			metric.WithUnit(unit.Dimensionless),
		),
		ExecutorCommitTime: meter.NewFloat64ValueRecorder(
			"executor.commit.time",
			metric.WithUnit(unit.Milliseconds),
		),

		ExecutorCommitWaitQueueTime: meter.NewFloat64ValueRecorder(
			"executor.commit_wait_queue.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorCommitWaitQueueTasks: meter.NewInt64ValueRecorder(
			"executor.commit_wait_queue.tasks",
			metric.WithUnit(unit.Dimensionless),
		),

		ExecutorReplicateQueueTime: meter.NewFloat64ValueRecorder(
			"executor.replicate_queue.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorReplicateTime: meter.NewFloat64ValueRecorder(
			"executor.replicate.time",
			metric.WithUnit(unit.Milliseconds),
		),
		/*
			ExecutorReplicateRequestPropagationTime: meter.NewFloat64ValueRecorder(
				"sn.executor.replicate.request.propagation.time",
				metric.WithUnit(unit.Milliseconds),
			),
			ExecutorReplicateResponsePropagationTime: meter.NewFloat64ValueRecorder(
				"sn.executor.replicate.response.propagation.time",
				metric.WithUnit(unit.Milliseconds),
			),
		*/
		ExecutorReplicateConnectionGetTime: meter.NewFloat64ValueRecorder(
			"sn.executor.replicate.connection.get.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorReplicateRequestPrepareTime: meter.NewFloat64ValueRecorder(
			"sn.executor.replicate.request.prepare.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorReplicateClientRequestQueueTime: meter.NewFloat64ValueRecorder(
			"sn.executor.replicate.client.request_queue.time",
			metric.WithUnit(unit.Milliseconds),
		),
		ExecutorReplicateFanoutTime: meter.NewFloat64ValueRecorder(
			"sn.executor.replicate.fanout.time",
			metric.WithUnit(unit.Milliseconds),
		),
	}
}
