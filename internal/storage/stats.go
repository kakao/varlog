package storage

import (
	"context"

	"github.com/cockroachdb/pebble"
)

// BatchCommitStats wraps [pebble.BatchCommitStats].
type BatchCommitStats struct {
	pebble.BatchCommitStats
}

// MetricRecorder is an interface for recording statistics for store.
type MetricRecorder interface {
	// RecordBatchCommitStats records the batch commit statistics.
	RecordBatchCommitStats(context.Context, BatchCommitStats)
}

var defaultMetricRecorder = &nopMetricRecorder{}

type nopMetricRecorder struct{}

var _ MetricRecorder = (*nopMetricRecorder)(nil)

func (*nopMetricRecorder) RecordBatchCommitStats(context.Context, BatchCommitStats) {}

// todoContext is a placeholder for the context used in metric recording.
var todoContext = context.TODO()
