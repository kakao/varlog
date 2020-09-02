package varlog

import "testing"

func TestLogStreamStatus(t *testing.T) {
	var tests = []struct {
		in  LogStreamStatus
		f   func(st LogStreamStatus) bool
		out bool
	}{
		{LogStreamStatusRunning, (LogStreamStatus).Deleted, false},
		{LogStreamStatusRunning, (LogStreamStatus).Running, true},
		{LogStreamStatusRunning, (LogStreamStatus).Sealed, false},

		{LogStreamStatusSealing, (LogStreamStatus).Deleted, false},
		{LogStreamStatusSealing, (LogStreamStatus).Running, false},
		{LogStreamStatusSealing, (LogStreamStatus).Sealed, true},

		{LogStreamStatusSealed, (LogStreamStatus).Deleted, false},
		{LogStreamStatusSealed, (LogStreamStatus).Running, false},
		{LogStreamStatusSealed, (LogStreamStatus).Sealed, true},

		{LogStreamStatusDeleted, (LogStreamStatus).Deleted, true},
		{LogStreamStatusDeleted, (LogStreamStatus).Running, false},
		{LogStreamStatusDeleted, (LogStreamStatus).Sealed, false},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.in.String(), func(t *testing.T) {
			s := tt.f(tt.in)
			if s != tt.out {
				t.Errorf("input=%v, expected=%v, actual=%v", tt.in, tt.out, s)
			}
		})
	}
}
