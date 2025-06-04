package storage

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/kakao/varlog/internal/stats/opentelemetry"
	"github.com/kakao/varlog/pkg/types"
)

type StoreKind int8

const (
	StoreKindValue StoreKind = iota
	StoreKindCommit
)

type BatchCommitDurationKind int8

const (
	BatchCommitDurationKindTotal BatchCommitDurationKind = iota
	BatchCommitDurationKindSemaphoreWait
	BatchCommitDurationKindWALQueueWait
	BatchCommitDurationKindMemTableWriteStall
	BatchCommitDurationKindL0ReadAmpWriteStall
	BatchCommitDurationKindWALRotation
	BatchCommitDurationKindCommitWait
)

func attributeStoreKind(storeKind StoreKind) attribute.KeyValue {
	var kind string
	switch storeKind {
	case StoreKindValue:
		kind = "value"
	case StoreKindCommit:
		kind = "commit"
	}
	return attribute.String("varlog.storage.store.kind", kind)
}

func attributeDurationKind(durationKind BatchCommitDurationKind) attribute.KeyValue {
	var kind string
	switch durationKind {
	case BatchCommitDurationKindTotal:
		kind = "total"
	case BatchCommitDurationKindSemaphoreWait:
		kind = "semaphore_wait"
	case BatchCommitDurationKindWALQueueWait:
		kind = "wal_queue_wait"
	case BatchCommitDurationKindMemTableWriteStall:
		kind = "mem_table_write_stall"
	case BatchCommitDurationKindL0ReadAmpWriteStall:
		kind = "l0_read_amp_write_stall"
	case BatchCommitDurationKindWALRotation:
		kind = "wal_rotation"
	case BatchCommitDurationKindCommitWait:
		kind = "commit_wait"
	}
	return attribute.String("varlog.storage.store.batch_commit.duration_kind", kind)
}

type otelMetricRecorder struct {
	// histograms for recording various durations related to batch commit stats
	totalDuration               *opentelemetry.Int64Histogram
	semaphoreWaitDuration       *opentelemetry.Int64Histogram
	walQueueWaitDuration        *opentelemetry.Int64Histogram
	memTableWriteStallDuration  *opentelemetry.Int64Histogram
	l0ReadAmpWriteStallDuration *opentelemetry.Int64Histogram
	walRotationDuration         *opentelemetry.Int64Histogram
	commitWaitDuration          *opentelemetry.Int64Histogram
}

var _ MetricRecorder = (*otelMetricRecorder)(nil)

func NewOTELMetricRecorder(histogramSet *opentelemetry.Int64HistogramSet[int64], lsid types.LogStreamID, storeKind StoreKind) *otelMetricRecorder {
	getHistogram := func(durationKind BatchCommitDurationKind) *opentelemetry.Int64Histogram {
		cacheKey := int64(lsid)<<32 | int64(storeKind)<<24 | int64(durationKind)<<16
		return histogramSet.GetHistogram(
			cacheKey,
			metric.WithAttributeSet(attribute.NewSet(
				opentelemetry.LogStreamID(lsid),
				attributeStoreKind(storeKind),
				attributeDurationKind(durationKind),
			)),
		)
	}

	return &otelMetricRecorder{
		totalDuration:               getHistogram(BatchCommitDurationKindTotal),
		semaphoreWaitDuration:       getHistogram(BatchCommitDurationKindSemaphoreWait),
		walQueueWaitDuration:        getHistogram(BatchCommitDurationKindWALQueueWait),
		memTableWriteStallDuration:  getHistogram(BatchCommitDurationKindMemTableWriteStall),
		l0ReadAmpWriteStallDuration: getHistogram(BatchCommitDurationKindL0ReadAmpWriteStall),
		walRotationDuration:         getHistogram(BatchCommitDurationKindWALRotation),
		commitWaitDuration:          getHistogram(BatchCommitDurationKindCommitWait),
	}
}

func (r *otelMetricRecorder) RecordBatchCommitStats(ctx context.Context, stats BatchCommitStats) {
	r.totalDuration.Record(ctx, stats.TotalDuration.Nanoseconds())
	r.semaphoreWaitDuration.Record(ctx, stats.SemaphoreWaitDuration.Nanoseconds())
	r.walQueueWaitDuration.Record(ctx, stats.WALQueueWaitDuration.Nanoseconds())
	r.memTableWriteStallDuration.Record(ctx, stats.MemTableWriteStallDuration.Nanoseconds())
	r.l0ReadAmpWriteStallDuration.Record(ctx, stats.L0ReadAmpWriteStallDuration.Nanoseconds())
	r.walRotationDuration.Record(ctx, stats.WALRotationDuration.Nanoseconds())
	r.commitWaitDuration.Record(ctx, stats.CommitWaitDuration.Nanoseconds())
}
