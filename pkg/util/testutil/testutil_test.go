package testutil_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/util/testutil"
)

func TestCompareWait(t *testing.T) {
	tcs := []struct {
		cmp  func() bool
		want bool
	}{
		{cmp: func() bool { return true }, want: true},
		{cmp: func() bool { return false }, want: false},
	}

	for _, tc := range tcs {
		got := testutil.CompareWait(tc.cmp, time.Second)
		require.Equal(t, tc.want, got)
	}
}

func TestCompareWaitN_Factor0(t *testing.T) {
	ts := time.Now()
	testutil.CompareWaitN(0, func() bool {
		return false
	})
	factor0 := time.Since(ts)

	ts = time.Now()
	testutil.CompareWaitN(1, func() bool {
		return false
	})
	factor1 := time.Since(ts)

	require.InEpsilon(t, 1.0, factor1/factor0, float64((10 * time.Millisecond).Nanoseconds()))
}

func TestCompareWaitN_Factor2(t *testing.T) {
	ts := time.Now()
	testutil.CompareWaitN(1, func() bool {
		return false
	})
	factor1 := time.Since(ts)

	ts = time.Now()
	testutil.CompareWaitN(2, func() bool {
		return false
	})
	factor2 := time.Since(ts)

	require.InEpsilon(t, 2.0, factor2/factor1, float64((10 * time.Millisecond).Nanoseconds()))
}

func TestGetFunctionName(t *testing.T) {
	got := testutil.GetFunctionName(TestGetFunctionName)
	require.Equal(t, "testutil_test.TestGetFunctionName", got)
}
