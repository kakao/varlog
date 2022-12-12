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
	fmt.Fprintf(&buf, "___tgt")  //  6 spaces
	fmt.Fprintf(&buf, "__arpsR") //  7 spaces
	fmt.Fprintf(&buf, "__arpsT") //  7 spaces

	fmt.Fprintf(&buf, "_______abpsR") // 12 spaces
	fmt.Fprintf(&buf, "_______abpsT") // 12 spaces

	fmt.Fprintf(&buf, "__adurR") //  7 spaces
	fmt.Fprintf(&buf, "__adurT") //  7 spaces

	fmt.Fprintf(&buf, "__slpsR") //  7 spaces
	fmt.Fprintf(&buf, "__slpsT") //  7 spaces

	fmt.Fprintf(&buf, "_______sbpsR") // 12 spaces
	fmt.Fprintf(&buf, "_______sbpsT") // 12 spaces

	fmt.Fprintf(&buf, "__eelatR")   //  8 spaces
	fmt.Fprintf(&buf, "__eelatT\n") //  8 spaces
	for idx, rpt := range trs.Reports {
		fmt.Fprintf(&buf, "%6s", rpt.Target)

		// arps
		fmt.Fprintf(&buf, "%7.1f", rpt.Recent.AppendReport.RequestsPerSecond)
		fmt.Fprintf(&buf, "%7.1f", rpt.Total.AppendReport.RequestsPerSecond)

		// abps
		fmt.Fprintf(&buf, "%10s/s", units.ToByteSizeString(rpt.Recent.AppendReport.BytesPerSecond))
		fmt.Fprintf(&buf, "%10s/s", units.ToByteSizeString(rpt.Total.AppendReport.BytesPerSecond))

		// adur
		fmt.Fprintf(&buf, "%7.1f", rpt.Recent.AppendReport.Duration)
		fmt.Fprintf(&buf, "%7.1f", rpt.Total.AppendReport.Duration)

		// slps
		fmt.Fprintf(&buf, "%7.1f", rpt.Recent.SubscribeReport.LogsPerSecond)
		fmt.Fprintf(&buf, "%7.1f", rpt.Total.SubscribeReport.LogsPerSecond)

		// sbps
		fmt.Fprintf(&buf, "%10s/s", units.ToByteSizeString(rpt.Recent.SubscribeReport.BytesPerSecond))
		fmt.Fprintf(&buf, "%10s/s", units.ToByteSizeString(rpt.Total.SubscribeReport.BytesPerSecond))

		// eelat
		fmt.Fprintf(&buf, "%8.1f", rpt.Recent.EndToEndReport.Latency)
		fmt.Fprintf(&buf, "%8.1f", rpt.Total.EndToEndReport.Latency)

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
