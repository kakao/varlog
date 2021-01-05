package storagenode

/*
type LogStreamExecutorMetrics struct {
	AppendCMetrics metrics.ChannelMetrics
	CommitCMetrics metrics.ChannelMetrics
	TrimCMetrics   metrics.ChannelMetrics

	AppendMetrics    OperationMetrics
	ReadMetrics      OperationMetrics
	SubscribeMetrics OperationMetrics
	TrimMetrics      OperationMetrics
	ReplicateMetrics OperationMetrics

	SyncMetrics struct {
	}
}

type OperationMetrics struct {
	Count    metrics.Counter
	BytesIn  metrics.Counter
	BytesOut metrics.Counter
	Latency  metrics.Guage

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
