package snpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncPositionInvalid(t *testing.T) {
	tcs := []struct {
		input    SyncPosition
		expected bool
	}{
		{
			input:    SyncPosition{LLSN: 1, GLSN: 0},
			expected: true,
		},
		{
			input:    SyncPosition{LLSN: 0, GLSN: 1},
			expected: true,
		},
		{
			input:    SyncPosition{LLSN: 0, GLSN: 0},
			expected: true,
		},
		{
			input:    SyncPosition{LLSN: 1, GLSN: 1},
			expected: false,
		},
	}

	for i := range tcs {
		tc := tcs[i]
		t.Run(tc.input.String(), func(t *testing.T) {
			if tc.input.Invalid() != tc.expected {
				t.Errorf("expected=%v, actual=%v", tc.expected, tc.input.Invalid())
			}
		})
	}
}

func TestSyncPositionInvalidConstructor(t *testing.T) {
	require.True(t, InvalidSyncPosition().Invalid())
}

func TestSyncPositionLessThan(t *testing.T) {
	tcs := []struct {
		left     SyncPosition
		right    SyncPosition
		expected bool
	}{
		{
			left:     SyncPosition{LLSN: 0, GLSN: 0},
			right:    SyncPosition{LLSN: 0, GLSN: 0},
			expected: false,
		},
		{
			left:     SyncPosition{LLSN: 0, GLSN: 0},
			right:    SyncPosition{LLSN: 0, GLSN: 1},
			expected: false,
		},
		{
			left:     SyncPosition{LLSN: 0, GLSN: 0},
			right:    SyncPosition{LLSN: 1, GLSN: 0},
			expected: false,
		},
		{
			left:     SyncPosition{LLSN: 0, GLSN: 0},
			right:    SyncPosition{LLSN: 1, GLSN: 1},
			expected: true,
		},
	}

	for i := range tcs {
		tc := tcs[i]
		t.Run(tc.left.String()+"_"+tc.right.String(), func(t *testing.T) {
			if tc.left.LessThan(tc.right) != tc.expected {
				t.Errorf("expected=%v, actual=%v", tc.expected, tc.left.LessThan(tc.right))
			}
		})
	}
}
