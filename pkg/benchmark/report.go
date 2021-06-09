package benchmark

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jstemmer/go-junit-report/formatter"
)

type record struct {
	err          error
	responseTime float64
	ts           time.Time
}

type report struct {
	duration time.Duration
	mu       sync.Mutex
	records  []record
}

func newReport(sizeHint int) *report {
	return &report{
		records: make([]record, 0, sizeHint),
	}
}

func (rpt *report) String() string {
	var sb strings.Builder
	if err := rpt.ToJUnit(&sb); err != nil {
		return err.Error()
	}
	return sb.String()
}

func (rpt *report) ToJUnit(w io.Writer) error {
	sort.Slice(rpt.records, func(i, j int) bool {
		return rpt.records[i].ts.Before(rpt.records[j].ts)
	})

	appendBench := make([]formatter.JUnitTestCase, 0, len(rpt.records))
	for _, rcd := range rpt.records {
		tc := formatter.JUnitTestCase{
			Classname: "benchmark",
			Name:      "Append",
			Time:      fmt.Sprintf("%f", rcd.responseTime),
		}
		if rcd.err != nil {
			tc.Failure = &formatter.JUnitFailure{
				Message:  rcd.err.Error(),
				Contents: fmt.Sprintf("%+v", rcd.err),
			}
		}
		appendBench = append(appendBench, tc)
	}

	suites := formatter.JUnitTestSuites{
		Suites: []formatter.JUnitTestSuite{
			{
				XMLName: xml.Name{
					Space: "github.com/kakao/varlog",
					Local: "benchmark",
				},
				Time:      fmt.Sprintf("%f", rpt.duration.Seconds()),
				Tests:     len(appendBench),
				TestCases: appendBench,
			},
		},
	}
	bytes, err := xml.MarshalIndent(suites, "", "\t")
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(w)
	writer.WriteString(xml.Header)
	writer.Write(bytes)
	writer.WriteString("\n")
	return writer.Flush()
}
