package benchmark

import (
	"encoding/json"
	"time"
)

type AppendReport struct {
	RequestsPerSecond float64 `json:"requestsPerSecond"`
	BytesPerSecond    float64 `json:"bytesPerSecond"`
	Duration          float64 `json:"duration"`
}

func NewAppendReportFromMetrics(metrics AppendMetrics, interval time.Duration) AppendReport {
	return AppendReport{
		RequestsPerSecond: float64(metrics.requests) / interval.Seconds(),
		BytesPerSecond:    float64(metrics.bytes) / interval.Seconds(),
		Duration:          float64(metrics.durationMS / metrics.requests),
	}
}

type SubscribeReport struct {
	LogsPerSecond  float64 `json:"logsPerSecond"`
	BytesPerSecond float64 `json:"bytesPerSecond"`
}

func NewSubscribeReportFromMetrics(metrics SubscribeMetrics, interval time.Duration) SubscribeReport {
	return SubscribeReport{
		LogsPerSecond:  float64(metrics.logs) / interval.Seconds(),
		BytesPerSecond: float64(metrics.bytes) / interval.Seconds(),
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
	Reports []TargetReport
}

func (trs TargetReports) String() string {
	enc := StringEncoder{}
	buf, err := enc.Encode(trs)
	if err != nil {
		panic(err)
	}
	return string(buf)
	////  arps: appended requests per second
	////  abps: appended megabytes per second
	////  adur: mean/min/max append duration in milliseconds
	////  slps: subscribed logs per second
	////  sbps: subscribed megabytes per second
	//// eelat: end-to-end latency in milliseconds
	//var sb strings.Builder
	//fmt.Fprintf(&sb, "___tgt")  //  6 spaces
	//fmt.Fprintf(&sb, "__arpsR") //  7 spaces
	//fmt.Fprintf(&sb, "__arpsT") //  7 spaces
	//
	//fmt.Fprintf(&sb, "_______abpsR") // 12 spaces
	//fmt.Fprintf(&sb, "_______abpsT") // 12 spaces
	//
	//fmt.Fprintf(&sb, "__adurR") //  7 spaces
	//fmt.Fprintf(&sb, "__adurT") //  7 spaces
	//
	//fmt.Fprintf(&sb, "__slpsR") //  7 spaces
	//fmt.Fprintf(&sb, "__slpsT") //  7 spaces
	//
	//fmt.Fprintf(&sb, "_______sbpsR") // 12 spaces
	//fmt.Fprintf(&sb, "_______sbpsT") // 12 spaces
	//
	//fmt.Fprintf(&sb, "__eelatR")   //  8 spaces
	//fmt.Fprintf(&sb, "__eelatT\n") //  8 spaces
	//for idx, rpt := range trs.Reports {
	//    fmt.Fprintf(&sb, "%6s", rpt.Target)
	//
	//    // arps
	//    fmt.Fprintf(&sb, "%7.1f", rpt.Recent.AppendReport.RequestsPerSecond)
	//    fmt.Fprintf(&sb, "%7.1f", rpt.Total.AppendReport.RequestsPerSecond)
	//
	//    // abps
	//    fmt.Fprintf(&sb, "%10s/s", units.ToByteSizeString(rpt.Recent.AppendReport.BytesPerSecond))
	//    fmt.Fprintf(&sb, "%10s/s", units.ToByteSizeString(rpt.Total.AppendReport.BytesPerSecond))
	//
	//    // adur
	//    fmt.Fprintf(&sb, "%7.1f", rpt.Recent.AppendReport.Duration)
	//    fmt.Fprintf(&sb, "%7.1f", rpt.Total.AppendReport.Duration)
	//
	//    // slps
	//    fmt.Fprintf(&sb, "%7.1f", rpt.Recent.SubscribeReport.LogsPerSecond)
	//    fmt.Fprintf(&sb, "%7.1f", rpt.Total.SubscribeReport.LogsPerSecond)
	//
	//    // sbps
	//    fmt.Fprintf(&sb, "%10s/s", units.ToByteSizeString(rpt.Recent.SubscribeReport.BytesPerSecond))
	//    fmt.Fprintf(&sb, "%10s/s", units.ToByteSizeString(rpt.Total.SubscribeReport.BytesPerSecond))
	//
	//    // eelat
	//    fmt.Fprintf(&sb, "%8.1f", rpt.Recent.EndToEndReport.Latency)
	//    fmt.Fprintf(&sb, "%8.1f", rpt.Total.EndToEndReport.Latency)
	//
	//    if idx < len(trs.Reports)-1 {
	//        fmt.Fprint(&sb, "\n")
	//    }
	//}
	//return sb.String()
}

func (trs TargetReports) JSON() ([]byte, error) {
	return json.Marshal(trs)
}
