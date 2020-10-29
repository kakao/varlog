package varlogpb

import (
	"sort"
	"strconv"
	"testing"
)

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

func TestDiffReplicaDescriptorSet(t *testing.T) {
	var tests = []struct {
		xs       []*ReplicaDescriptor
		ys       []*ReplicaDescriptor
		expected []*ReplicaDescriptor
	}{}

	for i := range tests {
		test := tests[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			actual := DiffReplicaDescriptorSet(test.xs, test.ys)
			if len(actual) != len(test.expected) {
				t.Error("size mismatch")
			}
			sort.Slice(actual, func(i, j int) bool {
				return actual[i].String() < actual[j].String()
			})
			sort.Slice(test.expected, func(i, j int) bool {
				return test.expected[i].String() < test.expected[j].String()
			})
			for i := 0; i < len(actual); i++ {
				x, y := actual[i], test.expected[i]
				if !x.Equal(y) {
					t.Errorf("inequal element: actual=%v expected=%v", x.String(), y.String())
				}
			}
		})
	}

}
