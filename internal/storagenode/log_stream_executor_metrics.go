package storagenode

/*
type LogStreamExecutorMetrics struct {
	AppendCMetrics metric.ChannelMetrics
	CommitCMetrics metric.ChannelMetrics
	TrimCMetrics   metric.ChannelMetrics

	AppendMetrics    OperationMetrics
	ReadMetrics      OperationMetrics
	SubscribeMetrics OperationMetrics
	TrimMetrics      OperationMetrics
	ReplicateMetrics OperationMetrics

	SyncMetrics struct {
	}
}

type OperationMetrics struct {
	Count    metric.Counter
	BytesIn  metric.Counter
	BytesOut metric.Counter
	Latency  metric.Guage

	startTime time.Time
}

func (m *OperationMetrics) Start() {
	m.Count.Add(1)
	m.startTime = time.Now()
}

func (m *OperationMetrics) Stop() {
	m.Count.Add(-1)
	m.Latency.Update(time.Since(m.startTime))
}
*/
