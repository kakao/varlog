package telemetry

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/unit"
)

type Metrics struct {
	RPCServerAppendDuration    metric.Float64Histogram
	RPCServerReplicateDuration metric.Float64Histogram

	// Write
	WriteQueueTime  metric.Float64Histogram
	WriteQueueTasks metric.Int64UpDownCounter

	WriteTime      metric.Float64Histogram
	WriteBatchSize metric.Int64Histogram

	WriteReadyTime metric.Int64Histogram

	WritePureTime        metric.Int64Histogram
	WriteFanoutTime      metric.Int64Histogram
	WriteFanoutWriteTime metric.Int64Histogram
	WriteFanoutSendTime  metric.Int64Histogram

	// Commit
	CommitQueueTime  metric.Float64Histogram
	CommitQueueTasks metric.Int64UpDownCounter

	CommitTime      metric.Float64Histogram
	CommitBatchSize metric.Int64Histogram

	CommitWaitQueueTime  metric.Float64Histogram
	CommitWaitQueueTasks metric.Int64UpDownCounter

	// Replicate
	ReplicateQueueTime  metric.Float64Histogram
	ReplicateQueueTasks metric.Int64UpDownCounter

	// NOTE: Should we need this metric? Replication can be fire-and-forget RPC.
	ReplicateTime metric.Float64Histogram

	ReplicateClientRequestQueueTime  metric.Float64Histogram
	ReplicateClientRequestQueueTasks metric.Int64UpDownCounter

	ReplicateServerRequestQueueTime   metric.Float64Histogram
	ReplicateServerRequestQueueTasks  metric.Int64UpDownCounter
	ReplicateServerResponseQueueTime  metric.Float64Histogram
	ReplicateServerResponseQueueTasks metric.Int64UpDownCounter

	ReplicateRequestPropagationTime  metric.Float64Histogram
	ReplicateResponsePropagationTime metric.Float64Histogram

	// Reports
	Reports            metric.Int64Counter
	Commits            metric.Int64Counter
	ReportedLogEntries metric.Int64Histogram
}

func NewMetrics() *Metrics {
	meter := global.Meter("varlogsn")
	metrics := &Metrics{}
	var err error
	metrics.RPCServerAppendDuration, err = meter.NewFloat64Histogram(
		"sn.rpc.server.append.duration",
		metric.WithDescription("the duration of the append request in milliseconds"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.RPCServerReplicateDuration, err = meter.NewFloat64Histogram(
		"sn.rpc.server.replicate.duration",
		metric.WithDescription("the duration of the replicate request in milliseconds"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.WriteQueueTime, err = meter.NewFloat64Histogram(
		"sn.write_queue.time",
		metric.WithDescription("queue latency in milliseconds that the write task is waiting to be handled"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.WriteQueueTasks, err = meter.NewInt64UpDownCounter(
		"sn.write_queue.tasks",
		metric.WithDescription("number of tasks in write queue"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.WriteTime, err = meter.NewFloat64Histogram(
		"sn.write.time",
		metric.WithDescription("the duration in milliseconds that write task is handled"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.WriteReadyTime, err = meter.NewInt64Histogram("sn.write_ready.time")
	if err != nil {
		panic(err)
	}

	metrics.WritePureTime, err = meter.NewInt64Histogram("sn.write_pure.time")
	if err != nil {
		panic(err)
	}

	metrics.WriteFanoutTime, err = meter.NewInt64Histogram("sn.write_fanout.time")
	if err != nil {
		panic(err)
	}
	metrics.WriteFanoutWriteTime, err = meter.NewInt64Histogram("sn.write_fanout.write.time")
	if err != nil {
		panic(err)
	}
	metrics.WriteFanoutSendTime, err = meter.NewInt64Histogram("sn.write_fanout.send.time")
	if err != nil {
		panic(err)
	}

	metrics.WriteBatchSize, err = meter.NewInt64Histogram(
		"sn.write_batch_size",
		metric.WithDescription("size of write batch"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.CommitQueueTime, err = meter.NewFloat64Histogram(
		"sn.commit_queue.time",
		metric.WithDescription("queue latency in milliseconds that the commit task is waiting to be handled"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.CommitQueueTasks, err = meter.NewInt64UpDownCounter(
		"sn.commit_queue.tasks",
		metric.WithDescription("number of tasks in commit queue"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.CommitTime, err = meter.NewFloat64Histogram(
		"sn.commit.time",
		metric.WithDescription("the duration in milliseconds that commit task is handled"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.CommitBatchSize, err = meter.NewInt64Histogram(
		"sn.commit_batch.size",
		metric.WithDescription("size of commit batch"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.CommitWaitQueueTime, err = meter.NewFloat64Histogram(
		"sn.commit_wait_queue.time",
		metric.WithDescription("queue latency in milliseconds that the commit wait task is waiting to be handled"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.CommitWaitQueueTasks, err = meter.NewInt64UpDownCounter(
		"sn.commit_wait_queue.tasks",
		metric.WithDescription("number of tasks in commit wait queue"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.ReplicateQueueTime, err = meter.NewFloat64Histogram(
		"sn.replicate_queue.time",
		metric.WithDescription("queue latency in milliseconds that the replicate task is waiting to be handled"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ReplicateQueueTasks, err = meter.NewInt64UpDownCounter(
		"sn.replicate_queue.tasks",
		metric.WithDescription("number of tasks in replicate queue in replicator"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.ReplicateTime, err = meter.NewFloat64Histogram(
		"sn.replicate.time",
		metric.WithDescription("response time of replicate in the primary server in milliseconds"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ReplicateClientRequestQueueTime, err = meter.NewFloat64Histogram(
		"sn.replicate_client.request_queue.time",
		metric.WithDescription("queue latency in milliseconds that the replicate request is waiting to be handled in replicate client"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.ReplicateClientRequestQueueTasks, err = meter.NewInt64UpDownCounter(
		"sn.replicate_client.request_queue.tasks",
		metric.WithDescription("number of tasks in request queue in replicate client"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.ReplicateServerRequestQueueTime, err = meter.NewFloat64Histogram(
		"sn.replicate_server.request_queue.time",
		metric.WithDescription("queue latency in milliseconds that thr replicate task is waiting to be handled in replicate server"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}
	metrics.ReplicateServerRequestQueueTasks, err = meter.NewInt64UpDownCounter(
		"sn.replicate_server.request_queue.tasks",
		metric.WithDescription("number of tasks in request queue in replicate server"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.ReplicateServerResponseQueueTime, err = meter.NewFloat64Histogram(
		"sn.replicate_server.response_queue.time",
		metric.WithDescription("queue latency in milliseconds that thr replicate task is waiting to be sent in replicate server"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}
	metrics.ReplicateServerResponseQueueTasks, err = meter.NewInt64UpDownCounter(
		"sn.replicate_server.response_queue.tasks",
		metric.WithDescription("number of tasks in response queue in replicate server"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.ReplicateRequestPropagationTime, err = meter.NewFloat64Histogram(
		"sn.replicate_request.propagation.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}
	metrics.ReplicateResponsePropagationTime, err = meter.NewFloat64Histogram(
		"sn.replicate_response.propagation.time",
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		panic(err)
	}

	metrics.Reports, err = meter.NewInt64Counter(
		"sn.reports",
		metric.WithDescription("number of reports"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}
	metrics.Commits, err = meter.NewInt64Counter(
		"sn.commits",
		metric.WithDescription("number of commits"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	metrics.ReportedLogEntries, err = meter.NewInt64Histogram(
		"sn.reported.log_entries",
		metric.WithDescription("number of log entries reported"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		panic(err)
	}

	return metrics
}
