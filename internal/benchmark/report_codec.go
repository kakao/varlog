package benchmark

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/kakao/varlog/pkg/util/units"
)

type ReportEncoder interface {
	Encode(trs TargetReports) ([]byte, error)
}

type JsonEncoder struct{}

func (je JsonEncoder) Encode(trs TargetReports) ([]byte, error) {
	return json.Marshal(trs)
}

type StringEncoder struct{}

func (se StringEncoder) Encode(trs TargetReports) ([]byte, error) {
	var buf bytes.Buffer
	//  arps: appended requests per second
	//  abps: appended megabytes per second
	//  adur: mean/min/max append duration in milliseconds
	//  slps: subscribed logs per second
	//  sbps: subscribed megabytes per second
	// eelat: end-to-end latency in milliseconds
	fmt.Fprintf(&buf, "target")                 //  6 spaces
	fmt.Fprintf(&buf, "_____arps(now,tot)")     // 18 spaces
	fmt.Fprintf(&buf, "_________abps(now,tot)") // 22 spaces
	fmt.Fprintf(&buf, "_adur(now,tot)")         // 14 spaces

	fmt.Fprintf(&buf, "_____slps(now,tot)")     // 18 spaces
	fmt.Fprintf(&buf, "_________sbps(now,tot)") // 22 spaces
	fmt.Fprintf(&buf, "__eelat(now,tot)")       // 16 spaces
	fmt.Fprintf(&buf, "\n")

	for idx, rpt := range trs.Reports {
		fmt.Fprintf(&buf, "%6s", rpt.Target)

		// arps
		fmt.Fprintf(&buf, "%7s/s", units.ToHumanSizeStringWithoutUnit(rpt.Recent.AppendReport.RequestsPerSecond, 4))
		fmt.Fprintf(&buf, "%7s/s", units.ToHumanSizeStringWithoutUnit(rpt.Total.AppendReport.RequestsPerSecond, 4))

		// abps
		fmt.Fprintf(&buf, "%9s/s", units.ToByteSizeString(rpt.Recent.AppendReport.BytesPerSecond))
		fmt.Fprintf(&buf, "%9s/s", units.ToByteSizeString(rpt.Total.AppendReport.BytesPerSecond))

		// adur
		fmt.Fprintf(&buf, "%5.1fms", rpt.Recent.AppendReport.Duration)
		fmt.Fprintf(&buf, "%5.1fms", rpt.Total.AppendReport.Duration)

		// slps
		fmt.Fprintf(&buf, "%7s/s", units.ToHumanSizeStringWithoutUnit(rpt.Recent.SubscribeReport.LogsPerSecond, 4))
		fmt.Fprintf(&buf, "%7s/s", units.ToHumanSizeStringWithoutUnit(rpt.Total.SubscribeReport.LogsPerSecond, 4))

		// sbps
		fmt.Fprintf(&buf, "%9s/s", units.ToByteSizeString(rpt.Recent.SubscribeReport.BytesPerSecond))
		fmt.Fprintf(&buf, "%9s/s", units.ToByteSizeString(rpt.Total.SubscribeReport.BytesPerSecond))

		// eelat
		fmt.Fprintf(&buf, "%6.1fms", rpt.Recent.EndToEndReport.Latency)
		fmt.Fprintf(&buf, "%6.1fms", rpt.Total.EndToEndReport.Latency)

		if idx < len(trs.Reports)-1 {
			fmt.Fprint(&buf, "\n")
		}
	}
	return buf.Bytes(), nil
}

func MustEncode(enc ReportEncoder, trs TargetReports) string {
	buf, err := enc.Encode(trs)
	if err != nil {
		panic(err)
	}
	return string(buf)
}
