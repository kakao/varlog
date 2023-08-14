package benchmark

import (
	"time"
)

type AppendReport struct {
	RequestsPerSecond float64 `json:"requestsPerSecond"`
	BytesPerSecond    float64 `json:"bytesPerSecond"`
	Duration          float64 `json:"duration"`
}

func NewAppendReportFromMetrics(metrics AppendMetrics, interval time.Duration) AppendReport {
	var rps, bps, dur float64
	if itv := interval.Seconds(); itv > 0 {
		rps = float64(metrics.requests) / itv
		bps = float64(metrics.bytes) / itv
	}
	if metrics.requests > 0 {
		dur = metrics.durationMS / float64(metrics.requests)
	}
	return AppendReport{
		RequestsPerSecond: rps,
		BytesPerSecond:    bps,
		Duration:          dur,
	}
}

type SubscribeReport struct {
	LogsPerSecond  float64 `json:"logsPerSecond"`
	BytesPerSecond float64 `json:"bytesPerSecond"`
}

func NewSubscribeReportFromMetrics(metrics SubscribeMetrics, interval time.Duration) SubscribeReport {
	var lps, bps float64
	if itv := interval.Seconds(); itv > 0 {
		lps = float64(metrics.logs) / itv
		bps = float64(metrics.bytes) / itv
	}
	return SubscribeReport{
		LogsPerSecond:  lps,
		BytesPerSecond: bps,
	}
}

type EndToEndReport struct {
	Latency float64 `json:"latency"`
}

type Report struct {
	AppendReport    `json:"append"`
	SubscribeReport `json:"subscribe"`
	EndToEndReport  `json:"endToEnd"`
}

type TargetReport struct {
	Target string `json:"target"`
	Recent Report `json:"recent"`
	Total  Report `json:"total"`
}

type TargetReports struct {
	Reports []TargetReport `json:"reports"`
}
