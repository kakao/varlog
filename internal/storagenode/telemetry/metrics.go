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
	semconv "go.opentelemetry.io/otel/semconv/v1.32.0"
	"google.golang.org/grpc/codes"

	"github.com/kakao/varlog/internal/stats/opentelemetry"
	"github.com/kakao/varlog/pkg/types"
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

	// LogRPCServerLogEntrySize records the size of individual log entries
	// appended. It is useful for tracking the amount of data being processed
	// and stored in the Varlog system.
	//
	// Internally, it is a wrapper of Metrics.logRPCServerLogEntrySize with
	// attributes cache. It avoids creating a new attribute set for each
	// observation.
	LogRPCServerLogEntrySize *opentelemetry.Int64Histogram

	// LogRPCServerBatchSize records the size of log entry batches appended.
	// This metric helps to understand the batch sizes being handled and
	// optimize the batching process in the Varlog system.
	//
	// Internally, it is a wrapper of Metrics.logRPCServerBatchSize with
	// attributes cache. It avoids creating a new attribute set for each
	// observation.
	LogRPCServerBatchSize *opentelemetry.Int64Histogram

	// LogRPCServerLogEntriesPerBatch records the number of log entries per
	// appended batch. It provides insights into the batch processing
	// efficiency and the average number of log entries per batch in the Varlog
	// system.
	//
	// Internally, it is a wrapper of Metrics.logRPCServerLogEntriesPerBatch
	// with attributes cache. It avoids creating a new attribute set for each
	// observation.
	LogRPCServerLogEntriesPerBatch *opentelemetry.Int64Histogram

	AppendPreparationDuration *opentelemetry.Int64Histogram

	SequencerOperationDuration  *opentelemetry.Int64Histogram
	SequencerFanoutDuration     *opentelemetry.Int64Histogram
	SequencerInflightOperations atomic.Int64

	WriterOperationDuration  *opentelemetry.Int64Histogram
	WriterInflightOperations atomic.Int64

	CommitterOperationDuration *opentelemetry.Int64Histogram
	CommitterLogs              *opentelemetry.Int64Histogram

	ReplicateClientOperationDuration  *opentelemetry.Int64Histogram
	ReplicateClientInflightOperations atomic.Int64

	ReplicateServerOperations atomic.Int64

	ReplicateDuration       *opentelemetry.Int64Histogram
	ReplicateFanoutDuration *opentelemetry.Int64Histogram
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
				semconv.RPCService(ServiceNames[rpcKind]),
				semconv.RPCMethod(MethodNames[rpcKind]),
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

var ServiceNames = []string{
	"varlog.snpb.LogIO",
	"varlog.snpb.Replicator",
}

var MethodNames = []string{
	"Append",
	"Replicate",
}

// Metrics are a set of measurements taken in the storage node. They encompass
// all measurements taken from each log stream.
type Metrics struct {
	metricsMap sync.Map // map[types.LogStreamID]*LogStreamMetrics

	// LogRPCServerDuration measures the time spent processing inbound RPC
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
	LogRPCServerDuration *opentelemetry.Int64HistogramSet[types.LogStreamID]

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
	logRPCServerLogEntrySize *opentelemetry.Int64HistogramSet[types.LogStreamID]

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
	logRPCServerBatchSize *opentelemetry.Int64HistogramSet[types.LogStreamID]

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
	logRPCServerLogEntriesPerBatch *opentelemetry.Int64HistogramSet[types.LogStreamID]

	// appendPreparationDuration measures the time spent preparing append operation in microseconds.
	appendPreparationDuration *opentelemetry.Int64HistogramSet[types.LogStreamID]

	sequencerOperationDuration *opentelemetry.Int64HistogramSet[types.LogStreamID]

	sequencerFanoutDuration *opentelemetry.Int64HistogramSet[types.LogStreamID]

	writerOperationDuration *opentelemetry.Int64HistogramSet[types.LogStreamID]

	committerOperationDuration *opentelemetry.Int64HistogramSet[types.LogStreamID]

	committerLogs *opentelemetry.Int64HistogramSet[types.LogStreamID]

	replicateClientOperationDuration *opentelemetry.Int64HistogramSet[types.LogStreamID]

	replicateDuration *opentelemetry.Int64HistogramSet[types.LogStreamID]

	replicateFanoutDuration *opentelemetry.Int64HistogramSet[types.LogStreamID]
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
	m.LogRPCServerDuration, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"log_rpc.server.duration",
		metric.WithDescription("Time spent processing inbound RPC in microseconds."),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}

	boundaries = nil
	// 10us, 20us, 30us, ..., 1000us
	for dur := 10 * time.Microsecond; dur <= 1000*time.Microsecond; dur += 10 * time.Microsecond {
		boundaries = append(boundaries, float64(dur.Nanoseconds())/float64(time.Microsecond))
	}
	// 1050us, 1100us, 1150us, ..., 5000us
	for dur := 1050 * time.Microsecond; dur <= 5000*time.Microsecond; dur += 50 * time.Microsecond {
		boundaries = append(boundaries, float64(dur.Nanoseconds())/float64(time.Microsecond))
	}
	// 5100us, 5200us, 5300us, ..., 10000us
	for dur := 5100 * time.Microsecond; dur <= 10000*time.Microsecond; dur += 100 * time.Microsecond {
		boundaries = append(boundaries, float64(dur.Nanoseconds())/float64(time.Microsecond))
	}
	// 11000us, 12000us, 13000us, ..., 50000us
	for dur := 11000 * time.Microsecond; dur <= 50000*time.Microsecond; dur += 1000 * time.Microsecond {
		boundaries = append(boundaries, float64(dur.Nanoseconds())/float64(time.Microsecond))
	}
	m.appendPreparationDuration, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"sn.append.preparation",
		metric.WithDescription("Time spent preparing append operation in microseconds"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}
	m.sequencerOperationDuration, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"sn.sequencer.operation.duration",
		metric.WithDescription("Time spent in sequencer operation in microseconds"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}
	m.sequencerFanoutDuration, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"sn.sequencer.fanout.duration",
		metric.WithDescription("Time spent in sequencer fanout in microseconds"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}
	m.writerOperationDuration, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"sn.writer.operation.duration",
		metric.WithDescription("Time spent in writer operation in microseconds"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}
	m.committerOperationDuration, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"sn.committer.operation.duration",
		metric.WithDescription("Time spent in committer operation in microseconds"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}
	m.replicateClientOperationDuration, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"sn.replicate.client.operation.duration",
		metric.WithDescription("Time spent in replicate client operation in microseconds"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}
	m.replicateDuration, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"sn.replicate.duration",
		metric.WithDescription("Time spent replicating from the log stream in microseconds"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}
	m.replicateFanoutDuration, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"sn.replicate.fanout.duration",
		metric.WithDescription("Time spent in replicate server fanout in microseconds"),
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
	m.logRPCServerLogEntrySize, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"log_rpc.server.log_entry.size",
		metric.WithDescription("Size of appended log entries."),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}

	m.logRPCServerBatchSize, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"log_rpc.server.batch.size",
		metric.WithDescription("Size of appended log entry batches."),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(boundaries...),
	)
	if err != nil {
		return nil, err
	}

	m.logRPCServerLogEntriesPerBatch, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
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
	m.committerLogs, err = opentelemetry.NewInt64HistogramSet[types.LogStreamID](
		meter,
		"sn.committer.logs",
		metric.WithDescription("Number of logs committed"),
		metric.WithUnit("{count}"),
		metric.WithExplicitBucketBoundaries(
			1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
		),
	)
	if err != nil {
		return nil, err
	}

	var (
		sequencerInflightOperations       metric.Int64ObservableGauge
		writerInflightOperations          metric.Int64ObservableGauge
		replicateClientInflightOperations metric.Int64ObservableGauge
		replicateServerOperations         metric.Int64ObservableCounter

		mu sync.Mutex
	)

	sequencerInflightOperations, err = meter.Int64ObservableGauge(
		"sn.sequencer.inflight.operations",
		metric.WithDescription("Number of sequencer operations in flight"),
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

	mu.Lock()
	defer mu.Unlock()

	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		mu.Lock()
		defer mu.Unlock()

		m.metricsMap.Range(func(key, value any) bool {
			lsm := value.(*LogStreamMetrics)
			observer.ObserveInt64(sequencerInflightOperations, lsm.SequencerInflightOperations.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(writerInflightOperations, lsm.WriterInflightOperations.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(replicateClientInflightOperations, lsm.ReplicateClientInflightOperations.Load(), metric.WithAttributeSet(lsm.attrs))
			observer.ObserveInt64(replicateServerOperations, lsm.ReplicateServerOperations.Load(), metric.WithAttributeSet(lsm.attrs))
			return true
		})

		return nil
	}, sequencerInflightOperations,
		writerInflightOperations,
		replicateClientInflightOperations,
		replicateServerOperations,
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
		opentelemetry.TopicID(tpid),
		opentelemetry.LogStreamID(lsid),
	)
	kvs := []attribute.KeyValue{
		opentelemetry.TopicID(tpid),
		opentelemetry.LogStreamID(lsid),
		semconv.RPCSystemGRPC,
	}
	lsm, loaded := m.metricsMap.LoadOrStore(lsid, &LogStreamMetrics{
		attrs:                            attrs,
		LogRPCServerLogEntrySize:         m.logRPCServerLogEntrySize.GetHistogram(lsid, metric.WithAttributes(kvs...)),
		LogRPCServerBatchSize:            m.logRPCServerBatchSize.GetHistogram(lsid, metric.WithAttributes(kvs...)),
		LogRPCServerLogEntriesPerBatch:   m.logRPCServerLogEntriesPerBatch.GetHistogram(lsid, metric.WithAttributes(kvs...)),
		AppendPreparationDuration:        m.appendPreparationDuration.GetHistogram(lsid, metric.WithAttributeSet(attrs)),
		SequencerOperationDuration:       m.sequencerOperationDuration.GetHistogram(lsid, metric.WithAttributeSet(attrs)),
		SequencerFanoutDuration:          m.sequencerFanoutDuration.GetHistogram(lsid, metric.WithAttributeSet(attrs)),
		WriterOperationDuration:          m.writerOperationDuration.GetHistogram(lsid, metric.WithAttributeSet(attrs)),
		CommitterOperationDuration:       m.committerOperationDuration.GetHistogram(lsid, metric.WithAttributeSet(attrs)),
		CommitterLogs:                    m.committerLogs.GetHistogram(lsid, metric.WithAttributeSet(attrs)),
		ReplicateClientOperationDuration: m.replicateClientOperationDuration.GetHistogram(lsid, metric.WithAttributeSet(attrs)),
		ReplicateDuration:                m.replicateDuration.GetHistogram(lsid, metric.WithAttributeSet(attrs)),
		ReplicateFanoutDuration:          m.replicateFanoutDuration.GetHistogram(lsid, metric.WithAttributeSet(attrs)),
	})
	if loaded {
		return nil, fmt.Errorf("storagenode: already registered %v", lsid)
	}
	return lsm.(*LogStreamMetrics), nil
}

func UnregisterLogStreamMetrics(m *Metrics, lsid types.LogStreamID) {
	m.metricsMap.Delete(lsid)
}
