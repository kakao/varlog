package benchmark

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kakao/varlog/pkg/util/units"
)

type Metrics struct {
	loaderMetrics []*LoaderMetrics
}

func (m Metrics) String() string {
	//  arps: appended requests per second
	//  abps: appended megabytes per second
	//  adur: mean/min/max append duration in milliseconds
	//  slps: subscribed logs per second
	//  sbps: subscribed megabytes per second
	// eelat: end-to-end latency in milliseconds
	var sb strings.Builder
	fmt.Fprintf(&sb, "___tgt")  //  6 spaces
	fmt.Fprintf(&sb, "__arpsR") //  7 spaces
	fmt.Fprintf(&sb, "__arpsT") //  7 spaces

	fmt.Fprintf(&sb, "_______abpsR") // 12 spaces
	fmt.Fprintf(&sb, "_______abpsT") // 12 spaces

	fmt.Fprintf(&sb, "__adurR") //  7 spaces
	fmt.Fprintf(&sb, "__adurT") //  7 spaces

	fmt.Fprintf(&sb, "__slpsR") //  7 spaces
	fmt.Fprintf(&sb, "__slpsT") //  7 spaces

	fmt.Fprintf(&sb, "_______sbpsR") // 12 spaces
	fmt.Fprintf(&sb, "_______sbpsT") // 12 spaces

	fmt.Fprintf(&sb, "__eelatR")   //  8 spaces
	fmt.Fprintf(&sb, "__eelatT\n") //  8 spaces
	for idx, lm := range m.loaderMetrics {
		recent, total := lm.Flush()
		fmt.Fprintf(&sb, "%6s", lm.tgt)

		// arps
		fmt.Fprintf(&sb, "%7.1f", recent.AppendReport.RequestsPerSecond)
		fmt.Fprintf(&sb, "%7.1f", total.AppendReport.RequestsPerSecond)

		// abps
		fmt.Fprintf(&sb, "%10s/s", units.ToByteSizeString(recent.AppendReport.BytesPerSecond))
		fmt.Fprintf(&sb, "%10s/s", units.ToByteSizeString(total.AppendReport.BytesPerSecond))

		// adur
		fmt.Fprintf(&sb, "%7.1f", recent.AppendReport.Duration)
		fmt.Fprintf(&sb, "%7.1f", total.AppendReport.Duration)

		// slps
		fmt.Fprintf(&sb, "%7.1f", recent.SubscribeReport.LogsPerSecond)
		fmt.Fprintf(&sb, "%7.1f", total.SubscribeReport.LogsPerSecond)

		// sbps
		fmt.Fprintf(&sb, "%10s/s", units.ToByteSizeString(recent.SubscribeReport.BytesPerSecond))
		fmt.Fprintf(&sb, "%10s/s", units.ToByteSizeString(total.SubscribeReport.BytesPerSecond))

		// eelat
		fmt.Fprintf(&sb, "%8.1f", recent.EndToEndReport.Latency)
		fmt.Fprintf(&sb, "%8.1f", total.EndToEndReport.Latency)

		if idx < len(m.loaderMetrics)-1 {
			fmt.Fprint(&sb, "\n")
		}
	}
	return sb.String()
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
	durationMS int64
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

func (lm *LoaderMetrics) Flush() (recent, total Report) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()

	total.AppendReport = NewAppendReportFromMetrics(lm.appendMetrics.total, now.Sub(lm.initTime))
	total.SubscribeReport = NewSubscribeReportFromMetrics(lm.subscribeMetrics.total, now.Sub(lm.initTime))

	recent.AppendReport = NewAppendReportFromMetrics(lm.appendMetrics.recent, now.Sub(lm.lastTime))
	recent.SubscribeReport = NewSubscribeReportFromMetrics(lm.subscribeMetrics.recent, now.Sub(lm.lastTime))

	lm.appendMetrics.recent = AppendMetrics{}
	lm.subscribeMetrics.recent = SubscribeMetrics{}
	lm.lastTime = now

	return recent, total
}
