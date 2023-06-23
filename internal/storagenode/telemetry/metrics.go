package telemetry

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/kakao/varlog/pkg/types"
)

type LogStreamMetrics struct {
	AppendLogs             atomic.Int64
	AppendBytes            atomic.Int64
	AppendDuration         atomic.Int64
	AppendOperations       atomic.Int64
	AppendPreparationMicro atomic.Int64
	AppendBatchCommitGap   atomic.Int64

	SequencerOperationDuration  atomic.Int64
	SequencerFanoutDuration     atomic.Int64
	SequencerOperations         atomic.Int64
	SequencerInflightOperations atomic.Int64

	WriterOperationDuration  atomic.Int64
	WriterOperations         atomic.Int64
	WriterInflightOperations atomic.Int64

	CommitterOperationDuration atomic.Int64
	CommitterOperations        atomic.Int64
	CommitterLogs              atomic.Int64

	ReplicateClientOperationDuration  atomic.Int64
	ReplicateClientOperations         atomic.Int64
	ReplicateClientInflightOperations atomic.Int64

	ReplicateServerOperations atomic.Int64

	ReplicateLogs             atomic.Int64
	ReplicateBytes            atomic.Int64
	ReplicateDuration         atomic.Int64
	ReplicateOperations       atomic.Int64
	ReplicatePreparationMicro atomic.Int64
}

type Metrics struct {
	snid       types.StorageNodeID
	metricsMap sync.Map
}

func RegisterMetrics(meter metric.Meter, snid types.StorageNodeID) (m *Metrics, err error) {
	m = &Metrics{snid: snid}

	var (
		appendLogs                    metric.Int64ObservableCounter
		appendBytes                   metric.Int64ObservableCounter
		appendDuration                metric.Int64ObservableCounter
		appendOperations              metric.Int64ObservableCounter
		appendPreparationMicroseconds metric.Int64ObservableCounter
		appendBatchCommitGap          metric.Int64ObservableCounter

		sequencerOperationDuration  metric.Int64ObservableCounter
		sequencerFanoutDuration     metric.Int64ObservableCounter
		sequencerOperations         metric.Int64ObservableCounter
		sequencerInflightOperations metric.Int64ObservableGauge

		writerOperationDuration  metric.Int64ObservableCounter
		writerOperations         metric.Int64ObservableCounter
		writerInflightOperations metric.Int64ObservableGauge

		committerOperationDuration metric.Int64ObservableCounter
		committerOperations        metric.Int64ObservableCounter
		committerLogs              metric.Int64ObservableCounter

		replicateClientOperationDuration  metric.Int64ObservableCounter
		replicateClientOperations         metric.Int64ObservableCounter
		replicateClientInflightOperations metric.Int64ObservableGauge

		replicateServerOperations metric.Int64ObservableCounter

		replicateLogs                    metric.Int64ObservableCounter
		replicateBytes                   metric.Int64ObservableCounter
		replicateDuration                metric.Int64ObservableCounter
		replicateOperations              metric.Int64ObservableCounter
		replicatePreparationMicroseconds metric.Int64ObservableCounter

		mu sync.Mutex
	)

	appendLogs, err = meter.Int64ObservableCounter(
		"sn.append.logs",
		metric.WithDescription("Number of logs appended to the log stream"),
	)
	if err != nil {
		return nil, err
	}

	appendLogs, err = meter.Int64ObservableCounter(
		"sn.append.logs",
		metric.WithDescription("Number of logs appended to the log stream"),
	)
	if err != nil {
		return nil, err
	}
	appendBytes, err = meter.Int64ObservableCounter(
		"sn.append.bytes",
		metric.WithDescription("Bytes appended to the log stream"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}
	appendDuration, err = meter.Int64ObservableCounter(
		"sn.append.duration",
		metric.WithDescription("Time spent appending to the log stream in microseconds"),
	)
	if err != nil {
		return nil, err
	}
	appendOperations, err = meter.Int64ObservableCounter(
		"sn.append.operations",
		metric.WithDescription("Number of append operations"),
	)
	if err != nil {
		return nil, err
	}
	appendPreparationMicroseconds, err = meter.Int64ObservableCounter(
		"sn.append.preparation.us",
		metric.WithDescription("Time spent preparing append operation in microseconds"),
	)
	if err != nil {
		return nil, err
	}
	appendBatchCommitGap, err = meter.Int64ObservableCounter(
		"sn.append.batch.commit.gap",
		metric.WithDescription("Time gap between the first and last commit in a batch"),
	)
	if err != nil {
		return nil, err
	}

	sequencerOperationDuration, err = meter.Int64ObservableCounter(
		"sn.sequencer.operation.duration",
		metric.WithDescription("Time spent in sequencer operation"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}
	sequencerFanoutDuration, err = meter.Int64ObservableCounter(
		"sn.sequencer.fanout.duration",
		metric.WithDescription("Time spent in sequencer fanout in microseconds"),
	)
	if err != nil {
		return nil, err
	}
	sequencerOperations, err = meter.Int64ObservableCounter(
		"sn.sequencer.operations",
		metric.WithDescription("Number of sequencer operations"),
	)
	if err != nil {
		return nil, err
	}
	sequencerInflightOperations, err = meter.Int64ObservableGauge(
		"sn.sequencer.inflight.operations",
		metric.WithDescription("Number of sequencer operations in flight"),
	)
	if err != nil {
		return nil, err
	}

	writerOperationDuration, err = meter.Int64ObservableCounter(
		"sn.writer.operation.duration",
		metric.WithDescription("Time spent in writer operation in microseconds"),
	)
	if err != nil {
		return nil, err
	}
	writerOperations, err = meter.Int64ObservableCounter(
		"sn.writer.operations",
		metric.WithDescription("Number of writer operations"),
	)
	if err != nil {
		return nil, err
	}
	writerInflightOperations, err = meter.Int64ObservableGauge(
		"sn.writer.inflight.operations",
		metric.WithDescription("Number of writer operations in flight"),
	)
	if err != nil {
		return nil, err
	}

	committerOperationDuration, err = meter.Int64ObservableCounter(
		"sn.committer.operation.duration",
		metric.WithDescription("Time spent in committer operation in microseconds"),
	)
	if err != nil {
		return nil, err
	}
	committerOperations, err = meter.Int64ObservableCounter(
		"sn.committer.operations",
		metric.WithDescription("Number of committer operations"),
	)
	if err != nil {
		return nil, err
	}
	committerLogs, err = meter.Int64ObservableCounter(
		"sn.committer.logs",
		metric.WithDescription("Number of logs committed"),
	)
	if err != nil {
		return nil, err
	}

	replicateClientOperationDuration, err = meter.Int64ObservableCounter(
		"sn.replicate.client.operation.duration",
		metric.WithDescription("Time spent in replicate client operation in microseconds"),
	)
	if err != nil {
		return nil, err
	}
	replicateClientOperations, err = meter.Int64ObservableCounter(
		"sn.replicate.client.operations",
		metric.WithDescription("Number of replicate client operations"),
	)
	if err != nil {
		return nil, err
	}
	replicateClientInflightOperations, err = meter.Int64ObservableGauge(
		"sn.replicate.client.inflight.operations",
		metric.WithDescription("Number of replicate client operations in flight"),
	)
	if err != nil {
		return nil, err
	}

	replicateServerOperations, err = meter.Int64ObservableCounter(
		"sn.replicate.server.operations",
		metric.WithDescription("Number of replicate server operations"),
	)
	if err != nil {
		return nil, err
	}

	replicateLogs, err = meter.Int64ObservableCounter(
		"sn.replicate.logs",
		metric.WithDescription("Number of logs replicated from the log stream"),
	)
	if err != nil {
		return nil, err
	}
	replicateBytes, err = meter.Int64ObservableCounter(
		"sn.replicate.bytes",
		metric.WithDescription("Bytes replicated from the log stream"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}
	replicateDuration, err = meter.Int64ObservableCounter(
		"sn.replicate.duration",
		metric.WithDescription("Time spent replicating from the log stream in microseconds"),
	)
	if err != nil {
		return nil, err
	}
	replicateOperations, err = meter.Int64ObservableCounter(
		"sn.replicate.operations",
		metric.WithDescription("Number of replicate operations"),
	)
	if err != nil {
		return nil, err
	}
	replicatePreparationMicroseconds, err = meter.Int64ObservableCounter(
		"sn.replicate.preparation.us",
		metric.WithDescription("Time spent preparing append operation"),
	)
	if err != nil {
		return nil, err
	}

	mu.Lock()
	defer mu.Unlock()

	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		mu.Lock()
		defer mu.Unlock()

		m.metricsMap.Range(func(key, value any) bool {
			lsid := key.(types.LogStreamID)
			lsm := value.(*LogStreamMetrics)
			attrs := attribute.NewSet(attribute.Int("lsid", int(lsid)))

			observer.ObserveInt64(appendLogs, lsm.AppendLogs.Load(), metric.WithAttributeSet(attrs))
			observer.ObserveInt64(appendBytes, lsm.AppendBytes.Load())
			observer.ObserveInt64(appendDuration, lsm.AppendDuration.Load())
			observer.ObserveInt64(appendOperations, lsm.AppendOperations.Load())
			observer.ObserveInt64(appendPreparationMicroseconds, lsm.AppendPreparationMicro.Load())
			observer.ObserveInt64(appendBatchCommitGap, lsm.AppendBatchCommitGap.Load())

			observer.ObserveInt64(sequencerOperationDuration, lsm.SequencerOperationDuration.Load())
			observer.ObserveInt64(sequencerFanoutDuration, lsm.SequencerFanoutDuration.Load())
			observer.ObserveInt64(sequencerOperations, lsm.SequencerOperations.Load())
			observer.ObserveInt64(sequencerInflightOperations, lsm.SequencerInflightOperations.Load())

			observer.ObserveInt64(writerOperationDuration, lsm.WriterOperationDuration.Load())
			observer.ObserveInt64(writerOperations, lsm.WriterOperations.Load())
			observer.ObserveInt64(writerInflightOperations, lsm.WriterInflightOperations.Load())

			observer.ObserveInt64(committerOperationDuration, lsm.CommitterOperationDuration.Load())
			observer.ObserveInt64(committerOperations, lsm.CommitterOperations.Load())
			observer.ObserveInt64(committerLogs, lsm.CommitterLogs.Load())

			observer.ObserveInt64(replicateClientOperationDuration, lsm.ReplicateClientOperationDuration.Load())
			observer.ObserveInt64(replicateClientOperations, lsm.ReplicateClientOperations.Load())
			observer.ObserveInt64(replicateClientInflightOperations, lsm.ReplicateClientInflightOperations.Load())

			observer.ObserveInt64(replicateServerOperations, lsm.ReplicateServerOperations.Load())

			observer.ObserveInt64(replicateLogs, lsm.ReplicateLogs.Load())
			observer.ObserveInt64(replicateBytes, lsm.ReplicateBytes.Load())
			observer.ObserveInt64(replicateDuration, lsm.ReplicateDuration.Load())
			observer.ObserveInt64(replicateOperations, lsm.ReplicateOperations.Load())
			observer.ObserveInt64(replicatePreparationMicroseconds, lsm.ReplicatePreparationMicro.Load())

			return true
		})

		return nil
	}, appendLogs, appendBytes, appendDuration, appendOperations, appendPreparationMicroseconds, appendBatchCommitGap,
		sequencerOperationDuration, sequencerFanoutDuration, sequencerOperations, sequencerInflightOperations,
		writerOperationDuration, writerOperations, writerInflightOperations,
		committerOperationDuration, committerOperations, committerLogs,
		replicateClientOperationDuration, replicateClientOperations, replicateClientInflightOperations,
		replicateServerOperations,
		replicateLogs, replicateBytes, replicateDuration, replicateOperations, replicatePreparationMicroseconds,
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func RegisterLogStreamMetrics(m *Metrics, lsid types.LogStreamID) (*LogStreamMetrics, error) {
	lsm, loaded := m.metricsMap.LoadOrStore(lsid, &LogStreamMetrics{})
	if loaded {
		return nil, fmt.Errorf("storagenode: already registered %v", lsid)
	}
	return lsm.(*LogStreamMetrics), nil
}

func UnregisterLogStreamMetrics(m *Metrics, lsid types.LogStreamID) {
	m.metricsMap.Delete(lsid)
}
