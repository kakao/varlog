package stringsutil

import (
	"testing"

	"go.uber.org/goleak"
)

func TestEmpty(t *testing.T) {
	var tests = []struct {
		in  string
		out bool
	}{
		{"", true},
		{" ", true},
		{"	", true},
		{"\n", true},
		{"a\n", false},
		{"\na", false},
		{"\t", true},
		{"a\t", false},
		{"\ta", false},
		{" a", false},
		{"a ", false},
		{"	a", false},
		{"a	", false},
		{"a", false},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			s := Empty(tt.in)
			if s != tt.out {
				t.Errorf("input=%v, expected=%v, actual=%v", tt.in, tt.out, s)
			}
		})
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
