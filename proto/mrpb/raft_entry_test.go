package mrpb

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
)

func TestReports(t *testing.T) {
	const nid = types.NodeID(1)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 1e4; i++ {
		reports := NewReports(nid, time.Now())
		require.Empty(t, reports.Reports)

		queue := NewReportQueue()
		require.Empty(t, queue)
		require.Equal(t, reportQueueSize, cap(queue))
		numReports := rng.Intn(reportQueueSize*2) + 1
		for j := 0; j < numReports; j++ {
			queue = append(queue, &Report{})
		}
		require.Len(t, queue, numReports)
		reports.Reports = queue

		reports.Release()
	}
}

func TestReportQueuePool(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 1e4; i++ {
		queue := NewReportQueue()
		require.Empty(t, queue)
		require.Equal(t, reportQueueSize, cap(queue))

		numReports := rng.Intn(reportQueueSize*2) + 1
		for j := 0; j < numReports; j++ {
			queue = append(queue, &Report{})
		}
		require.Len(t, queue, numReports)

		queue.Release()
	}
}

func BenchmarkReportQueuePool(b *testing.B) {
	const numReports = 128
	predefinedReports := make([]*Report, numReports)
	for i := 0; i < numReports; i++ {
		predefinedReports[i] = &Report{}
	}
	reports := &Reports{}

	reports.Reports = make([]*Report, 0, reportQueueSize)
	b.ResetTimer()
	b.Run("WithoutPool", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			for j := 0; j < numReports; j++ {
				reports.Reports = append(reports.Reports, predefinedReports[j])
			}
			reports.Reports = make([]*Report, 0, reportQueueSize)
		}
	})

	reports.Reports = NewReportQueue()
	b.ResetTimer()
	b.Run("WithPool", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			for j := 0; j < numReports; j++ {
				reports.Reports = append(reports.Reports, predefinedReports[j])
			}
			rq := (ReportQueue)(reports.Reports)
			rq.Release()
			reports.Reports = NewReportQueue()
		}
	})
}
