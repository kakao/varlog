package telemetry

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/unit"
)

type Metrics struct {
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

func NewMetrics() *Metrics {
	meter := global.Meter("varlog.sn")
	metrics := &Metrics{}
	var err error
	metrics.RPCServerAppendDuration, err = meter.NewFloat64Histogram(
		"rpc.server.append.duration",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.RPCServerReplicateDuration, err = meter.NewFloat64Histogram(
		"rpc.server.replicate.duration",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorWriteQueueTime, err = meter.NewFloat64Histogram(
		"executor.write_queue.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorWriteQueueTasks, err = meter.NewInt64Histogram(
		"executor.write_queue.tasks",
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorWriteTime, err = meter.NewFloat64Histogram(
		"executor.write.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorCommitQueueTime, err = meter.NewFloat64Histogram(
		"executor.commit_queue.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorCommitQueueTasks, err = meter.NewInt64Histogram(
		"executor.commit_queue.tasks",
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorCommitTime, err = meter.NewFloat64Histogram(
		"executor.commit.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorCommitWaitQueueTime, err = meter.NewFloat64Histogram(
		"executor.commit_wait_queue.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorCommitWaitQueueTasks, err = meter.NewInt64Histogram(
		"executor.commit_wait_queue.tasks",
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorReplicateQueueTime, err = meter.NewFloat64Histogram(
		"executor.replicate_queue.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorReplicateTime, err = meter.NewFloat64Histogram(
		"executor.replicate.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}
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
	metrics.ExecutorReplicateConnectionGetTime, err = meter.NewFloat64Histogram(
		"sn.executor.replicate.connection.get.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorReplicateRequestPrepareTime, err = meter.NewFloat64Histogram(
		"sn.executor.replicate.request.prepare.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorReplicateClientRequestQueueTime, err = meter.NewFloat64Histogram(
		"sn.executor.replicate.client.request_queue.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ExecutorReplicateFanoutTime, err = meter.NewFloat64Histogram(
		"sn.executor.replicate.fanout.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	return metrics
}
