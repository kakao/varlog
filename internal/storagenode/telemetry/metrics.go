package telemetry

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"google.golang.org/grpc/codes"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/telemetry"
)

// LogStreamMetrics is a set of metrics measured in each log stream.
//
// Synchronous metrics are of instrumentation types defined in OpenTelemetry,
// such as Int64Histogram.
//
// Asynchronous metrics are defined as built-in atomic types, meaning they are
// just counters. Each log stream increments these counters, which are then
// collected by a measurement cycle managed by OpenTelemetry.
type LogStreamMetrics struct {
	attrs attribute.Set

	// LogRPCServerDuration records the time spent processing inbound RPC calls
	// in microseconds. It helps to monitor and analyze the performance of RPC
	// calls in the Varlog system.
	//
	// Internally, it is a wrapper of Metrics.logRPCServerDuration with
	// attributes cache. It avoids creating a new attribute set for each
	// observation.
	LogRPCServerDuration *Int64HistogramRecorder

	// LogRPCServerLogEntrySize records the size of individual log entries
	// appended. It is useful for tracking the amount of data being processed
	// and stored in the Varlog system.
	//
	// Internally, it is a wrapper of Metrics.logRPCServerLogEntrySize with
	// attributes cache. It avoids creating a new attribute set for each
	// observation.
	LogRPCServerLogEntrySize *Int64HistogramRecorder

	// LogRPCServerBatchSize records the size of log entry batches appended.
	// This metric helps to understand the batch sizes being handled and
	// optimize the batching process in the Varlog system.
	//
	// Internally, it is a wrapper of Metrics.logRPCServerBatchSize with
	// attributes cache. It avoids creating a new attribute set for each
	// observation.
	LogRPCServerBatchSize *Int64HistogramRecorder

	// LogRPCServerLogEntriesPerBatch records the number of log entries per
	// appended batch. It provides insights into the batch processing
	// efficiency and the average number of log entries per batch in the Varlog
	// system.
	//
	// Internally, it is a wrapper of Metrics.logRPCServerLogEntriesPerBatch
	// with attributes cache. It avoids creating a new attribute set for each
	// observation.
	LogRPCServerLogEntriesPerBatch *Int64HistogramRecorder

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

type Int64HistogramRecorder struct {
	recorder     metric.Int64Histogram
	defaultAttrs []attribute.KeyValue
	options      map[uint32][]metric.RecordOption
	mu           *xsync.RBMutex
}

func NewInt64HistogramRecorder(recorder metric.Int64Histogram, defaultAttrs ...attribute.KeyValue) *Int64HistogramRecorder {
	return &Int64HistogramRecorder{
		recorder:     recorder,
		defaultAttrs: defaultAttrs,
		options:      make(map[uint32][]metric.RecordOption),
		mu:           xsync.NewRBMutex(),
	}
}

func (r *Int64HistogramRecorder) Record(ctx context.Context, rpcKind RPCKind, code codes.Code, incr int64) {
	key := uint32(rpcKind)<<16 | uint32(code)
	var opts []metric.RecordOption
	rt := r.mu.RLock()
	opts, ok := r.options[key]
	r.mu.RUnlock(rt)
	if !ok {
		r.mu.Lock()
		opts, ok = r.options[key]
		if !ok {
			attrs := slices.Concat(r.defaultAttrs, []attribute.KeyValue{
				semconv.RPCService(serviceNames[rpcKind]),
				semconv.RPCMethod(methodNames[rpcKind]),
				semconv.RPCGRPCStatusCodeKey.Int64(int64(code)),
			})
			r.options[key] = []metric.RecordOption{
				metric.WithAttributeSet(attribute.NewSet(attrs...)),
			}
		}
		r.mu.Unlock()
	}
	r.recorder.Record(ctx, incr, opts...)
}

type RPCKind uint8

const (
	RPCKindAppend RPCKind = iota
	RPCKindReplicate
)

var serviceNames = []string{
	"varlog.snpb.LogIO",
	"varlog.snpb.Replicator",
}

var methodNames = []string{
	"Append",
	"Replicate",
}

// Metrics are a set of measurements taken in the storage node. They encompass
// all measurements taken from each log stream.
type Metrics struct {
	metricsMap sync.Map // map[types.LogStreamID]*LogStreamMetrics

	// logRPCServerDuration measures the time spent processing inbound RPC
	// calls in microseconds. Its name is log_rpc.server.duration.
	//
	// It is similar to rpc.server.duration defined by OpenTelemetry, but it
	// differs from the specification in the following ways:
	//   - The unit is microseconds instead of milliseconds.
	//   - The bucket boundaries are customized.
	//   - It measures the processing time triggered by each request on the
	//   gRPC stream rather than the duration of the RPC itself.
	//   - It includes additional attributes related to Varlog.
	//
	// Attributes:
	//   - varlog.topic.id
	//   - varlog.logstream.id
	//   - rpc.system
	//   - rpc.method
	//   - rpc.service
	//   - rpc.grpc.status_code
	//
	// References:
	//   - https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/#metric-rpcserverduration
	//   - https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/#attributes
	//   - https://opentelemetry.io/docs/specs/semconv/rpc/grpc/#grpc-attributes
	//   - https://github.com/grpc/proposal/blob/master/A66-otel-stats.md#units
	logRPCServerDuration metric.Int64Histogram

	// logRPCServerLogEntrySize measures the sizes of appended log entries in
	// bytes. Its name is log_rpc.server.log_entry.size.
	//
	// It is similar to rpc.server.request.size defined by OpenTelemetry, but
	// it differs from the specification in the following ways:
	//   - The bucket boundaries are customized.
	//   - It measures the size of each log entry in the appended batch rather than the size of the request message itself.
	//   - It includes additional attributes related to Varlog.
	//
	// Attributes:
	//   - varlog.topic.id
	//   - varlog.logstream.id
	//   - rpc.system
	//   - rpc.method
	//   - rpc.service
	//   - rpc.grpc.status_code
	//
	// References:
	//   - https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/#metric-rpcserverrequestsize
	//   - https://github.com/grpc/proposal/blob/master/A66-otel-stats.md#units
	logRPCServerLogEntrySize metric.Int64Histogram

	// logRPCServerBatchSize measures the size of appended batches in bytes.
	// Its name is log_rpc.server.batch.size. It also includes additional
	// attributes related to Varlog.
	//
	// Attributes:
	//   - varlog.topic.id
	//   - varlog.logstream.id
	//   - rpc.system
	//   - rpc.method
	//   - rpc.service
	//   - rpc.grpc.status_code
	//
	// References:
	//   - https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/#metric-rpcserverrequestsize
	//   - https://github.com/grpc/proposal/blob/master/A66-otel-stats.md#units
	logRPCServerBatchSize metric.Int64Histogram

	// logRPCServerLogEntriesPerBatch measures the number of log entries per
	// batch appended. Its name is log_rpc.server.log_entries_per_batch.
	//
	// It is similar to rpc.server.requests_per_rpc defined by OpenTelemetry,
	// but it differs from the specification in the following ways:
	//   - The bucket boundaries are customized.
	//   - It measures the number of log entries in the appended batch rather
	//   than the number of requests on the gRPC stream.
	//   - It includes additional attributes related to Varlog.
	//
	// Attributes:
	//   - varlog.topic.id
	//   - varlog.logstream.id
	//   - rpc.system
	//   - rpc.method
	//   - rpc.service
	//   - rpc.grpc.status_code
	//
	// References:
	//   - https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/#metric-rpcserverrequests_per_rpc
	//   - https://github.com/grpc/proposal/blob/master/A66-otel-stats.md#units
	logRPCServerLogEntriesPerBatch metric.Int64Histogram
}

func RegisterMetrics(meter metric.Meter) (m *Metrics, err error) {
	m = &Metrics{}

	var boundaries []float64
	// 100us, 200us, ... 1000us
	for dur := 100 * time.Microsecond; dur <= 1000*time.Microsecond; dur += 100 * time.Microsecond {
		boundaries = append(boundaries, float64(dur.Nanoseconds())/1000.0)
	}
	// 2ms, 12ms, 14ms, ..., 100ms
	for dur := 2 * time.Millisecond; dur <= 100*time.Millisecond; dur += 2 * time.Millisecond {
		boundaries = append(boundaries, float64(dur.Nanoseconds())/1000.0)
	}
	// 150ms, 200ms, ... , 3000ms
	for dur := 150 * time.Millisecond; dur <= 3000*time.Millisecond; dur += 50 * time.Millisecond {
		boundaries = append(boundaries, float64(dur.Nanoseconds())/1000.0)
	}
	m.logRPCServerDuration, err = meter.Int64Histogram(
		"log_rpc.server.duration",
		metric.WithDescription("Time spent processing inbound RPC in microseconds."),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}

	boundaries = nil
	// 100B, 200B, 300B, ..., 1000B
	for sz := 100; sz <= 1000; sz += 100 {
		boundaries = append(boundaries, float64(sz))
	}
	// 5KB, 10KB, 15KB, ..., 100KB
	for sz := 5_000; sz <= 100_000; sz += 5_000 {
		boundaries = append(boundaries, float64(sz))
	}
	// 150KB, 200KB, ..., 1000KB (1MB)
	for sz := 150_000; sz <= 1000_000; sz += 50_000 {
		boundaries = append(boundaries, float64(sz))
	}
	// 1500KB, 2000KB, ..., 20000KB (20MB)
	for sz := 1500_000; sz <= 20_000_000; sz += 500_000 {
		boundaries = append(boundaries, float64(sz))
	}
	m.logRPCServerLogEntrySize, err = meter.Int64Histogram(
		"log_rpc.server.log_entry.size",
		metric.WithDescription("Size of appended log entries."),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}

	m.logRPCServerBatchSize, err = meter.Int64Histogram(
		"log_rpc.server.batch.size",
		metric.WithDescription("Size of appended log entry batches."),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}

	m.logRPCServerLogEntriesPerBatch, err = meter.Int64Histogram(
		"log_rpc.server.log_entries_per_batch",
		metric.WithDescription("Number of log entries per appended batch."),
		metric.WithUnit("{count}"),
		metric.WithExplicitBucketBoundaries(
			1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
		),
	)
	if err != nil {
		return nil, err
	}

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
			lsm := value.(*LogStreamMetrics)

			observer.ObserveInt64(appendLogs, lsm.AppendLogs.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(appendBytes, lsm.AppendBytes.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(appendDuration, lsm.AppendDuration.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(appendOperations, lsm.AppendOperations.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(appendPreparationMicroseconds, lsm.AppendPreparationMicro.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(appendBatchCommitGap, lsm.AppendBatchCommitGap.Load(), metric.WithAttributeSet(lsm.attrs))

			observer.ObserveInt64(sequencerOperationDuration, lsm.SequencerOperationDuration.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(sequencerFanoutDuration, lsm.SequencerFanoutDuration.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(sequencerOperations, lsm.SequencerOperations.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(sequencerInflightOperations, lsm.SequencerInflightOperations.Load(), metric.WithAttributeSet(lsm.attrs))

			observer.ObserveInt64(writerOperationDuration, lsm.WriterOperationDuration.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(writerOperations, lsm.WriterOperations.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(writerInflightOperations, lsm.WriterInflightOperations.Load(), metric.WithAttributeSet(lsm.attrs))

			observer.ObserveInt64(committerOperationDuration, lsm.CommitterOperationDuration.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(committerOperations, lsm.CommitterOperations.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(committerLogs, lsm.CommitterLogs.Load(), metric.WithAttributeSet(lsm.attrs))

			observer.ObserveInt64(replicateClientOperationDuration, lsm.ReplicateClientOperationDuration.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(replicateClientOperations, lsm.ReplicateClientOperations.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(replicateClientInflightOperations, lsm.ReplicateClientInflightOperations.Load(), metric.WithAttributeSet(lsm.attrs))

			observer.ObserveInt64(replicateServerOperations, lsm.ReplicateServerOperations.Load(), metric.WithAttributeSet(lsm.attrs))

			observer.ObserveInt64(replicateLogs, lsm.ReplicateLogs.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(replicateBytes, lsm.ReplicateBytes.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(replicateDuration, lsm.ReplicateDuration.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(replicateOperations, lsm.ReplicateOperations.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(replicatePreparationMicroseconds, lsm.ReplicatePreparationMicro.Load(), metric.WithAttributeSet(lsm.attrs))

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

func (m *Metrics) GetLogStreamMetrics(lsid types.LogStreamID) (*LogStreamMetrics, bool) {
	v, ok := m.metricsMap.Load(lsid)
	if !ok {
		return nil, false
	}
	return v.(*LogStreamMetrics), true
}

func RegisterLogStreamMetrics(m *Metrics, tpid types.TopicID, lsid types.LogStreamID) (*LogStreamMetrics, error) {
	attrs := attribute.NewSet(
		telemetry.TopicID(tpid),
		telemetry.LogStreamID(lsid),
	)
	kvs := []attribute.KeyValue{
		telemetry.TopicID(tpid),
		telemetry.LogStreamID(lsid),
		semconv.RPCSystemGRPC,
	}
	lsm, loaded := m.metricsMap.LoadOrStore(lsid, &LogStreamMetrics{
		attrs:                          attrs,
		LogRPCServerDuration:           NewInt64HistogramRecorder(m.logRPCServerDuration, kvs...),
		LogRPCServerLogEntrySize:       NewInt64HistogramRecorder(m.logRPCServerLogEntrySize, kvs...),
		LogRPCServerBatchSize:          NewInt64HistogramRecorder(m.logRPCServerBatchSize, kvs...),
		LogRPCServerLogEntriesPerBatch: NewInt64HistogramRecorder(m.logRPCServerLogEntriesPerBatch, kvs...),
	})
	if loaded {
		return nil, fmt.Errorf("storagenode: already registered %v", lsid)
	}
	return lsm.(*LogStreamMetrics), nil
}

func UnregisterLogStreamMetrics(m *Metrics, lsid types.LogStreamID) {
	m.metricsMap.Delete(lsid)
}
