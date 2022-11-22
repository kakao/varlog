package benchmark

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type Metrics struct {
	loaderMetrics []*LoaderMetrics
}

func (m Metrics) String() string {
	// __idx__: target index
	// _arps__: appended requests per second
	// _ambps_: appended megabytes per second
	// _adurs_: mean/min/max append duration in milliseconds
	// _slps__: subscribed logs per second
	// _smbps_: subscribed megabytes per second
	// _eelat_: end-to-end latency in milliseconds

	var sb strings.Builder
	for _, tm := range m.loaderMetrics {
		tm.Flush(&sb)
	}
	return sb.String()
}

type LoaderMetrics struct {
	idx int

	mu               sync.Mutex
	lastTime         time.Time
	appendMetrics    AppendMetrics
	subscribeMetrics SubscribeMetrics
}

type AppendMetrics struct {
	requests   int64
	bytes      int64
	durationMS int64
}

type SubscribeMetrics struct {
	logs  int64
	bytes int64
}

func (twm *LoaderMetrics) Reset(now time.Time) {
	twm.appendMetrics = AppendMetrics{}
	twm.subscribeMetrics = SubscribeMetrics{}
	twm.lastTime = now
}

func (twm *LoaderMetrics) ReportAppendMetrics(m AppendMetrics) bool {
	if !twm.mu.TryLock() {
		return false
	}
	defer twm.mu.Unlock()
	twm.appendMetrics.requests += m.requests
	twm.appendMetrics.bytes += m.bytes
	twm.appendMetrics.durationMS += m.durationMS
	return true
}

func (twm *LoaderMetrics) ReportSubscribeMetrics(m SubscribeMetrics) bool {
	if !twm.mu.TryLock() {
		return false
	}
	defer twm.mu.Unlock()
	twm.subscribeMetrics.logs += m.logs
	twm.subscribeMetrics.bytes += m.bytes
	return true
}

func (twm *LoaderMetrics) Flush(sb *strings.Builder) {
	const mib = 1 << 20

	twm.mu.Lock()
	defer twm.mu.Unlock()

	now := time.Now()
	interval := now.Sub(twm.lastTime).Seconds()

	// _arps__: appended requests per second
	// _ambps_: appended megabytes per second
	// _adurs_: mean/min/max append duration in milliseconds
	// _slps__: subscribed logs per second
	// _smbps_: subscribed megabytes per second
	// _eelat_: end-to-end latency in milliseconds

	fmt.Fprintf(sb, "idx=%2d\t", twm.idx)

	ap := &twm.appendMetrics
	fmt.Fprintf(sb, "arps=%2.1f\t", float64(ap.requests)/interval)
	fmt.Fprintf(sb, "abps=%7.1f\t", float64(ap.bytes)/interval/mib)

	sm := &twm.subscribeMetrics
	fmt.Fprintf(sb, "slps=%2.1f\t", float64(sm.logs)/interval)
	fmt.Fprintf(sb, "sbps=%7.1f", float64(sm.bytes)/interval/mib)

	twm.appendMetrics = AppendMetrics{}
	twm.subscribeMetrics = SubscribeMetrics{}
	twm.lastTime = now
}
