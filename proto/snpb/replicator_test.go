package snpb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
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

func TestInvalidSyncRange(t *testing.T) {
	sr := InvalidSyncRange()
	require.True(t, sr.Invalid())
}

func TestSyncRangeInvalid(t *testing.T) {
	tcs := []struct {
		sr       SyncRange
		expected bool
	}{
		{
			sr:       SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.InvalidLLSN},
			expected: true,
		},
		{
			sr:       SyncRange{FirstLLSN: types.LLSN(1), LastLLSN: types.InvalidLLSN},
			expected: true,
		},
		{
			sr:       SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.LLSN(1)},
			expected: true,
		},
		{
			sr:       SyncRange{FirstLLSN: types.LLSN(2), LastLLSN: types.LLSN(1)},
			expected: true,
		},
		{
			sr:       SyncRange{FirstLLSN: types.LLSN(1), LastLLSN: types.LLSN(2)},
			expected: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.sr.String(), func(t *testing.T) {
			require.Equal(t, tc.expected, tc.sr.Invalid())
		})
	}
}

func TestSyncRangeValidate(t *testing.T) {
	tcs := []struct {
		sr      SyncRange
		wantErr bool
	}{
		{
			sr:      SyncRange{FirstLLSN: types.LLSN(2), LastLLSN: types.LLSN(1)},
			wantErr: true,
		},
		{
			sr:      SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.LLSN(1)},
			wantErr: true,
		},
		{
			sr:      SyncRange{FirstLLSN: types.LLSN(1), LastLLSN: types.LLSN(2)},
			wantErr: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.sr.String(), func(t *testing.T) {
			err := tc.sr.Validate()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
