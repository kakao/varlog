package snpb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
)

func TestLogStreamUncommitReport_Invalid(t *testing.T) {
	tcs := []struct {
		report *snpb.LogStreamUncommitReport
		name   string
		want   bool
	}{
		{
			name: "ValidReport",
			report: &snpb.LogStreamUncommitReport{
				LogStreamID:           types.MinLogStreamID,
				UncommittedLLSNOffset: types.MinLLSN,
			},
			want: false,
		},
		{
			name: "InvalidLogStreamID",
			report: &snpb.LogStreamUncommitReport{
				LogStreamID:           types.LogStreamID(0),
				UncommittedLLSNOffset: types.MinLLSN,
			},
			want: true,
		},
		{
			name: "InvalidUncommittedLLSNOffset",
			report: &snpb.LogStreamUncommitReport{
				LogStreamID:           types.MinLogStreamID,
				UncommittedLLSNOffset: types.InvalidLLSN,
			},
			want: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.report.Invalid())
		})
	}
}

func TestLogStreamUncommitReport_UncommittedLLSNEnd(t *testing.T) {
	tcs := []struct {
		report *snpb.LogStreamUncommitReport
		name   string
		want   types.LLSN
	}{
		{
			name:   "NilReport",
			report: nil,
			want:   types.InvalidLLSN,
		},
		{
			name: "ValidReport",
			report: &snpb.LogStreamUncommitReport{
				UncommittedLLSNOffset: types.LLSN(10),
				UncommittedLLSNLength: 5,
			},
			want: types.LLSN(15),
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.report.UncommittedLLSNEnd())
		})
	}
}

func TestLogStreamUncommitReport_Seal(t *testing.T) {
	tcs := []struct {
		report     *snpb.LogStreamUncommitReport
		name       string
		end        types.LLSN
		want       types.LLSN
		wantLength uint64
	}{
		{
			name:       "NilReport",
			report:     nil,
			end:        types.LLSN(10),
			want:       types.InvalidLLSN,
			wantLength: 0,
		},
		{
			name: "EndLessThanUncommittedOffset",
			report: &snpb.LogStreamUncommitReport{
				UncommittedLLSNOffset: types.LLSN(10),
			},
			end:        types.LLSN(5),
			want:       types.InvalidLLSN,
			wantLength: 0,
		},
		{
			name: "EndGreaterThanUncommittedEnd",
			report: &snpb.LogStreamUncommitReport{
				UncommittedLLSNOffset: types.LLSN(10),
				UncommittedLLSNLength: 5,
			},
			end:        types.LLSN(20),
			want:       types.InvalidLLSN,
			wantLength: 5,
		},
		{
			name: "ValidSeal",
			report: &snpb.LogStreamUncommitReport{
				UncommittedLLSNOffset: types.LLSN(10),
				UncommittedLLSNLength: 5,
			},
			end:        types.LLSN(12),
			want:       types.LLSN(12),
			wantLength: 2,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.report.Seal(tc.end)
			assert.Equal(t, tc.want, got)
			if tc.report != nil {
				assert.Equal(t, tc.wantLength, tc.report.UncommittedLLSNLength)
			}
		})
	}
}
