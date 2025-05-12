package opentelemetry

import (
	"context"

	"github.com/puzpuzpuz/xsync/v2"
	"go.opentelemetry.io/otel/metric"
)

// Int64Histogram is a wrapper around OpenTelemetry's Int64Histogram. It allows
// recording values with predefined [metric.RecordOption].
type Int64Histogram struct {
	histogram metric.Int64Histogram
	opts      []metric.RecordOption
}

// Record adds a value to the histogram using the predefined options.
func (h *Int64Histogram) Record(ctx context.Context, incr int64) {
	h.histogram.Record(ctx, incr, h.opts...)
}

// Int64HistogramSet is a wrapper around OpenTelemetry's Int64Histogram. It
// includes a cache for [metric.RecordOption] to optimize performance by
// reducing redundant object creation for specific keys.
type Int64HistogramSet[K comparable] struct {
	histogram metric.Int64Histogram
	optsCache *recordOptionCache[K]
}

// NewInt64HistogramSet initializes a new Int64HistogramSet with the specified
// name and options.
func NewInt64HistogramSet[K comparable](meter metric.Meter, name string, opts ...metric.Int64HistogramOption) (*Int64HistogramSet[K], error) {
	histogram, err := meter.Int64Histogram(name, opts...)
	if err != nil {
		return nil, err
	}

	ret := &Int64HistogramSet[K]{
		histogram: histogram,
		optsCache: newRecordOptionCache[K](),
	}
	return ret, nil
}

// Record adds a value to the histogram for a specific key. It reuses cached
// [metric.RecordOption] if available or generates new options using getOpts.
func (hs *Int64HistogramSet[K]) Record(ctx context.Context, key K, incr int64, getOpts func() []metric.RecordOption) {
	record(ctx, hs.histogram, key, hs.optsCache, incr, getOpts)
}

// GetHistogram retrieves a [Int64Histogram] for the specified key, setting its
// [metric.RecordOption] in the cache for quick access.
func (hs *Int64HistogramSet[K]) GetHistogram(key K, opts ...metric.RecordOption) *Int64Histogram {
	hs.optsCache.mu.Lock()
	defer hs.optsCache.mu.Unlock()
	hs.optsCache.opts[key] = opts
	return &Int64Histogram{
		histogram: hs.histogram,
		opts:      opts,
	}
}

// Float64Histogram is a wrapper around OpenTelemetry's Float64Histogram. It
// allows recording values with predefined [metric.RecordOption].
type Float64Histogram struct {
	histogram metric.Float64Histogram
	opts      []metric.RecordOption
}

// Record adds a value to the histogram using the predefined options.
func (h *Float64Histogram) Record(ctx context.Context, incr float64) {
	h.histogram.Record(ctx, incr, h.opts...)
}

// Float64HistogramSet is a wrapper around OpenTelemetry's Float64Histogram. It
// includes a cache for [metric.RecordOption] to optimize performance by
// reducing redundant object creation for specific keys.
type Float64HistogramSet[K comparable] struct {
	histogram metric.Float64Histogram
	optsCache *recordOptionCache[K]
}

// NewFloat64HistogramSet initializes a new Float64HistogramSet with the
// specified name and options.
func NewFloat64HistogramSet[K comparable](meter metric.Meter, name string, opts ...metric.Float64HistogramOption) (*Float64HistogramSet[K], error) {
	histogram, err := meter.Float64Histogram(name, opts...)
	if err != nil {
		return nil, err
	}

	ret := &Float64HistogramSet[K]{
		histogram: histogram,
		optsCache: newRecordOptionCache[K](),
	}
	return ret, nil
}

// Record adds a value to the histogram for a specific key. It reuses cached
// [metric.RecordOption] if available or generates new options using getOpts.
func (hs *Float64HistogramSet[K]) Record(ctx context.Context, key K, incr float64, getOpts func() []metric.RecordOption) {
	record(ctx, hs.histogram, key, hs.optsCache, incr, getOpts)
}

// GetHistogram retrieves a [Float64Histogram] for the specified key, setting
// its [metric.RecordOption] in the cache for quick access.
func (hs *Float64HistogramSet[K]) GetHistogram(key K, opts ...metric.RecordOption) *Float64Histogram {
	hs.optsCache.mu.Lock()
	defer hs.optsCache.mu.Unlock()
	hs.optsCache.opts[key] = opts
	return &Float64Histogram{
		histogram: hs.histogram,
		opts:      opts,
	}
}

type recordOptionCache[K comparable] struct {
	opts map[K][]metric.RecordOption
	mu   *xsync.RBMutex
}

func newRecordOptionCache[K comparable]() *recordOptionCache[K] {
	return &recordOptionCache[K]{
		opts: make(map[K][]metric.RecordOption),
		mu:   xsync.NewRBMutex(),
	}
}

type recorder[T int64 | float64] interface {
	Record(ctx context.Context, incr T, options ...metric.RecordOption)
}

func record[K comparable, V int64 | float64](ctx context.Context, recorder recorder[V], key K, cache *recordOptionCache[K], incr V, getOpts func() []metric.RecordOption) {
	rt := cache.mu.RLock()
	if opts, ok := cache.opts[key]; ok {
		cache.mu.RUnlock(rt)
		recorder.Record(ctx, incr, opts...)
		return
	}
	cache.mu.RUnlock(rt)

	cache.mu.Lock()
	defer cache.mu.Unlock()
	opts, ok := cache.opts[key]
	if !ok {
		opts = getOpts()
		cache.opts[key] = opts
	}
	recorder.Record(ctx, incr, opts...)
}
