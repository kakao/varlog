package benchmark

import (
	"sync"
	"time"
)

type Metrics struct {
	loaderMetrics []*LoaderMetrics
}

func (m Metrics) Flush() TargetReports {
	trs := TargetReports{
		Reports: make([]TargetReport, len(m.loaderMetrics)),
	}
	for idx, lm := range m.loaderMetrics {
		trs.Reports[idx] = lm.Flush()
	}
	return trs
}

type LoaderMetrics struct {
	tgt Target

	mu            sync.Mutex
	initTime      time.Time
	lastTime      time.Time
	appendMetrics struct {
		total  AppendMetrics
		recent AppendMetrics
	}
	subscribeMetrics struct {
		total  SubscribeMetrics
		recent SubscribeMetrics
	}
}

type AppendMetrics struct {
	requests   int64
	bytes      int64
	durationMS float64
}

type SubscribeMetrics struct {
	logs  int64
	bytes int64
}

func (lm *LoaderMetrics) Reset(now time.Time) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.appendMetrics.total = AppendMetrics{}
	lm.appendMetrics.recent = AppendMetrics{}
	lm.subscribeMetrics.total = SubscribeMetrics{}
	lm.subscribeMetrics.recent = SubscribeMetrics{}
	lm.initTime = now
	lm.lastTime = now
}

func (lm *LoaderMetrics) ReportAppendMetrics(m AppendMetrics) bool {
	if !lm.mu.TryLock() {
		return false
	}
	defer lm.mu.Unlock()
	lm.appendMetrics.recent.requests += m.requests
	lm.appendMetrics.recent.bytes += m.bytes
	lm.appendMetrics.recent.durationMS += m.durationMS
	lm.appendMetrics.total.requests += m.requests
	lm.appendMetrics.total.bytes += m.bytes
	lm.appendMetrics.total.durationMS += m.durationMS
	return true
}

func (lm *LoaderMetrics) ReportSubscribeMetrics(m SubscribeMetrics) bool {
	if !lm.mu.TryLock() {
		return false
	}
	defer lm.mu.Unlock()
	lm.subscribeMetrics.recent.logs += m.logs
	lm.subscribeMetrics.recent.bytes += m.bytes
	lm.subscribeMetrics.total.logs += m.logs
	lm.subscribeMetrics.total.bytes += m.bytes
	return true
}

func (lm *LoaderMetrics) Flush() TargetReport {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()
	var recent, total Report

	total.AppendReport = NewAppendReportFromMetrics(lm.appendMetrics.total, now.Sub(lm.initTime))
	total.SubscribeReport = NewSubscribeReportFromMetrics(lm.subscribeMetrics.total, now.Sub(lm.initTime))

	recent.AppendReport = NewAppendReportFromMetrics(lm.appendMetrics.recent, now.Sub(lm.lastTime))
	recent.SubscribeReport = NewSubscribeReportFromMetrics(lm.subscribeMetrics.recent, now.Sub(lm.lastTime))

	lm.appendMetrics.recent = AppendMetrics{}
	lm.subscribeMetrics.recent = SubscribeMetrics{}
	lm.lastTime = now

	return TargetReport{
		Target: lm.tgt.String(),
		Recent: recent,
		Total:  total,
	}
}
