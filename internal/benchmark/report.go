package benchmark

import (
	"time"
)

type AppendReport struct {
	RequestsPerSecond float64
	BytesPerSecond    float64
	Duration          float64
}

type SubscribeReport struct {
	LogsPerSecond  float64
	BytesPerSecond float64
}

type EndToEndReport struct {
	Latency float64
}

type Report struct {
	AppendReport
	SubscribeReport
	EndToEndReport
}

func NewAppendReportFromMetrics(metrics AppendMetrics, interval time.Duration) AppendReport {
	return AppendReport{
		RequestsPerSecond: float64(metrics.requests) / interval.Seconds(),
		BytesPerSecond:    float64(metrics.bytes) / interval.Seconds(),
		Duration:          float64(metrics.durationMS / metrics.requests),
	}
}

func NewSubscribeReportFromMetrics(metrics SubscribeMetrics, interval time.Duration) SubscribeReport {
	return SubscribeReport{
		LogsPerSecond:  float64(metrics.logs) / interval.Seconds(),
		BytesPerSecond: float64(metrics.bytes) / interval.Seconds(),
	}
}
