package telemetry

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/unit"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type LogStreamMetrics struct {
	AppendLogs             int64
	AppendBytes            int64
	AppendDuration         int64
	AppendOperations       int64
	AppendPreparationMicro int64
	AppendBatchCommitGap   int64

	SequencerOperationDuration  int64
	SequencerFanoutDuration     int64
	SequencerOperations         int64
	SequencerInflightOperations int64

	WriterOperationDuration  int64
	WriterOperations         int64
	WriterInflightOperations int64

	CommitterOperationDuration int64
	CommitterOperations        int64
	CommitterLogs              int64

	ReplicateClientOperationDuration  int64
	ReplicateClientOperations         int64
	ReplicateClientInflightOperations int64

	ReplicateServerOperations int64

	ReplicateLogs             int64
	ReplicateBytes            int64
	ReplicateDuration         int64
	ReplicateOperations       int64
	ReplicatePreparationMicro int64
}

type Metrics struct {
	snid       types.StorageNodeID
	metricsMap sync.Map
}

func RegisterMetrics(meter metric.Meter, snid types.StorageNodeID) (m *Metrics, err error) {
	m = &Metrics{snid: snid}

	var (
		appendLogs                    metric.Int64CounterObserver
		appendBytes                   metric.Int64CounterObserver
		appendDuration                metric.Int64CounterObserver
		appendOperations              metric.Int64CounterObserver
		appendPreparationMicroseconds metric.Int64CounterObserver
		appendBatchCommitGap          metric.Int64CounterObserver

		sequencerOperationDuration  metric.Int64CounterObserver
		sequencerFanoutDuration     metric.Int64CounterObserver
		sequencerOperations         metric.Int64CounterObserver
		sequencerInflightOperations metric.Int64GaugeObserver

		writerOperationDuration  metric.Int64CounterObserver
		writerOperations         metric.Int64CounterObserver
		writerInflightOperations metric.Int64GaugeObserver

		committerOperationDuration metric.Int64CounterObserver
		committerOperations        metric.Int64CounterObserver
		committerLogs              metric.Int64CounterObserver

		replicateClientOperationDuration  metric.Int64CounterObserver
		replicateClientOperations         metric.Int64CounterObserver
		replicateClientInflightOperations metric.Int64GaugeObserver

		replicateServerOperations metric.Int64CounterObserver

		replicateLogs                    metric.Int64CounterObserver
		replicateBytes                   metric.Int64CounterObserver
		replicateDuration                metric.Int64CounterObserver
		replicateOperations              metric.Int64CounterObserver
		replicatePreparationMicroseconds metric.Int64CounterObserver

		mu sync.Mutex
	)

	mu.Lock()
	defer mu.Unlock()

	batchObserver := meter.NewBatchObserver(func(ctx context.Context, result metric.BatchObserverResult) {
		mu.Lock()
		defer mu.Unlock()

		m.metricsMap.Range(func(key, value interface{}) bool {
			lsid := key.(types.LogStreamID)
			lsm := value.(*LogStreamMetrics)
			attrs := []attribute.KeyValue{
				attribute.Int("lsid", int(lsid)),
			}

			result.Observe(attrs,
				appendLogs.Observation(atomic.LoadInt64(&lsm.AppendLogs)),
				appendBytes.Observation(atomic.LoadInt64(&lsm.AppendBytes)),
				appendDuration.Observation(atomic.LoadInt64(&lsm.AppendDuration)),
				appendOperations.Observation(atomic.LoadInt64(&lsm.AppendOperations)),
				appendPreparationMicroseconds.Observation(atomic.LoadInt64(&lsm.AppendPreparationMicro)),
				appendBatchCommitGap.Observation(atomic.LoadInt64(&lsm.AppendBatchCommitGap)),

				sequencerOperationDuration.Observation(atomic.LoadInt64(&lsm.SequencerOperationDuration)),
				sequencerFanoutDuration.Observation(atomic.LoadInt64(&lsm.SequencerFanoutDuration)),
				sequencerOperations.Observation(atomic.LoadInt64(&lsm.SequencerOperations)),
				sequencerInflightOperations.Observation(atomic.LoadInt64(&lsm.SequencerInflightOperations)),

				writerOperationDuration.Observation(atomic.LoadInt64(&lsm.WriterOperationDuration)),
				writerOperations.Observation(atomic.LoadInt64(&lsm.WriterOperations)),
				writerInflightOperations.Observation(atomic.LoadInt64(&lsm.WriterInflightOperations)),

				committerOperationDuration.Observation(atomic.LoadInt64(&lsm.CommitterOperationDuration)),
				committerOperations.Observation(atomic.LoadInt64(&lsm.CommitterOperations)),
				committerLogs.Observation(atomic.LoadInt64(&lsm.CommitterLogs)),

				replicateClientOperationDuration.Observation(atomic.LoadInt64(&lsm.ReplicateClientOperationDuration)),
				replicateClientOperations.Observation(atomic.LoadInt64(&lsm.ReplicateClientOperations)),
				replicateClientInflightOperations.Observation(atomic.LoadInt64(&lsm.ReplicateClientInflightOperations)),

				replicateServerOperations.Observation(atomic.LoadInt64(&lsm.ReplicateServerOperations)),

				replicateLogs.Observation(atomic.LoadInt64(&lsm.ReplicateLogs)),
				replicateBytes.Observation(atomic.LoadInt64(&lsm.ReplicateBytes)),
				replicateDuration.Observation(atomic.LoadInt64(&lsm.ReplicateDuration)),
				replicateOperations.Observation(atomic.LoadInt64(&lsm.ReplicateOperations)),
				replicatePreparationMicroseconds.Observation(atomic.LoadInt64(&lsm.ReplicatePreparationMicro)),
			)

			return true
		})
	})

	appendLogs, err = batchObserver.NewInt64CounterObserver(
		"sn.append.logs",
		metric.WithDescription("Number of logs appended to the log stream"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}
	appendBytes, err = batchObserver.NewInt64CounterObserver(
		"sn.append.bytes",
		metric.WithDescription("Bytes appended to the log stream"),
		metric.WithUnit(unit.Bytes),
	)
	if err != nil {
		return nil, err
	}
	appendDuration, err = batchObserver.NewInt64CounterObserver(
		"sn.append.duration",
		metric.WithDescription("Time spent appending to the log stream"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		return nil, err
	}
	appendOperations, err = batchObserver.NewInt64CounterObserver(
		"sn.append.operations",
		metric.WithDescription("Number of append operations"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}
	appendPreparationMicroseconds, err = batchObserver.NewInt64CounterObserver(
		"sn.append.preparation.us",
		metric.WithDescription("Time spent preparing append operation"),
	)
	if err != nil {
		return nil, err
	}
	appendBatchCommitGap, err = batchObserver.NewInt64CounterObserver(
		"sn.append.batch.commit.gap",
		metric.WithDescription("Time gap between the first and last commit in a batch"),
	)
	if err != nil {
		return nil, err
	}

	sequencerOperationDuration, err = batchObserver.NewInt64CounterObserver(
		"sn.sequencer.operation.duration",
		metric.WithDescription("Time spent in sequencer operation"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		return nil, err
	}
	sequencerFanoutDuration, err = batchObserver.NewInt64CounterObserver(
		"sn.sequencer.fanout.duration",
		metric.WithDescription("Time spent in sequencer fanout"),
	)
	if err != nil {
		return nil, err
	}
	sequencerOperations, err = batchObserver.NewInt64CounterObserver(
		"sn.sequencer.operations",
		metric.WithDescription("Number of sequencer operations"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}
	sequencerInflightOperations, err = batchObserver.NewInt64GaugeObserver(
		"sn.sequencer.inflight.operations",
		metric.WithDescription("Number of sequencer operations in flight"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}

	writerOperationDuration, err = batchObserver.NewInt64CounterObserver(
		"sn.writer.operation.duration",
		metric.WithDescription("Time spent in writer operation"),
	)
	if err != nil {
		return nil, err
	}
	writerOperations, err = batchObserver.NewInt64CounterObserver(
		"sn.writer.operations",
		metric.WithDescription("Number of writer operations"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}
	writerInflightOperations, err = batchObserver.NewInt64GaugeObserver(
		"sn.writer.inflight.operations",
		metric.WithDescription("Number of writer operations in flight"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}

	committerOperationDuration, err = batchObserver.NewInt64CounterObserver(
		"sn.committer.operation.duration",
		metric.WithDescription("Time spent in committer operation"),
	)
	if err != nil {
		return nil, err
	}
	committerOperations, err = batchObserver.NewInt64CounterObserver(
		"sn.committer.operations",
		metric.WithDescription("Number of committer operations"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}
	committerLogs, err = batchObserver.NewInt64CounterObserver(
		"sn.committer.logs",
		metric.WithDescription("Number of logs committed"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}

	replicateClientOperationDuration, err = batchObserver.NewInt64CounterObserver(
		"sn.replicate.client.operation.duration",
		metric.WithDescription("Time spent in replicate client operation"),
	)
	if err != nil {
		return nil, err
	}
	replicateClientOperations, err = batchObserver.NewInt64CounterObserver(
		"sn.replicate.client.operations",
		metric.WithDescription("Number of replicate client operations"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}
	replicateClientInflightOperations, err = batchObserver.NewInt64GaugeObserver(
		"sn.replicate.client.inflight.operations",
		metric.WithDescription("Number of replicate client operations in flight"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}

	replicateServerOperations, err = batchObserver.NewInt64CounterObserver(
		"sn.replicate.server.operations",
		metric.WithDescription("Number of replicate server operations"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}

	replicateLogs, err = batchObserver.NewInt64CounterObserver(
		"sn.replicate.logs",
		metric.WithDescription("Number of logs replicated from the log stream"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}
	replicateBytes, err = batchObserver.NewInt64CounterObserver(
		"sn.replicate.bytes",
		metric.WithDescription("Bytes replicated from the log stream"),
		metric.WithUnit(unit.Bytes),
	)
	if err != nil {
		return nil, err
	}
	replicateDuration, err = batchObserver.NewInt64CounterObserver(
		"sn.replicate.duration",
		metric.WithDescription("Time spent replicating from the log stream"),
		metric.WithUnit(unit.Milliseconds),
	)
	if err != nil {
		return nil, err
	}
	replicateOperations, err = batchObserver.NewInt64CounterObserver(
		"sn.replicate.operations",
		metric.WithDescription("Number of replicate operations"),
		metric.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, err
	}
	replicatePreparationMicroseconds, err = batchObserver.NewInt64CounterObserver(
		"sn.replicate.preparation.us",
		metric.WithDescription("Time spent preparing append operation"),
		metric.WithUnit(unit.Dimensionless),
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
