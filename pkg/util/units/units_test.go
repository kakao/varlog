package units

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestFromByteSizeString(t *testing.T) {
	var size int64
	var err error

	size, err = FromByteSizeString("1G")
	require.NoError(t, err)
	require.EqualValues(t, 1<<30, size)

	size, err = FromByteSizeString("0G")
	require.NoError(t, err)
	require.EqualValues(t, 0, size)

	_, err = FromByteSizeString("-1G")
	require.Error(t, err)

	_, err = FromByteSizeString("1G", 0, 1<<10)
	require.Error(t, err)

	_, err = FromByteSizeString("1KB", 1<<20)
	require.Error(t, err)
}

func TestToByteSizeString(t *testing.T) {
	const sizeInBytes = 1024
	size, err := FromByteSizeString(ToByteSizeString(sizeInBytes))
	require.NoError(t, err)
	require.EqualValues(t, sizeInBytes, size)
}

func TestToHumanSizeStringWithoutUnit(t *testing.T) {
	tcs := []struct {
		size      float64
		precision int
		want      string
	}{
		{size: 0, precision: 0, want: "0"},
		{size: 0, precision: 1, want: "0"},
		{size: 1.49, precision: 0, want: "1"},
		{size: 1.49, precision: 1, want: "1"},
		{size: 1.49, precision: 2, want: "1.5"},
		{size: 1.44, precision: 2, want: "1.4"},
		{size: 1.50, precision: 1, want: "2"},
		{size: 1.50, precision: 2, want: "1.5"},
		{size: 1 << 10, precision: 1, want: "1k"},
		{size: 10 << 10, precision: 1, want: "1e+01k"},
		{size: 10 << 10, precision: 2, want: "10k"},
		{size: 1 << 20, precision: 1, want: "1M"},
		{size: 1 << 30, precision: 1, want: "1G"},
		{size: 1 << 40, precision: 1, want: "1T"},
		{size: 1 << 50, precision: 1, want: "1P"},
		{size: 123456.78, precision: 6, want: "123.457k"},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.want, func(t *testing.T) {
			got := ToHumanSizeStringWithoutUnit(tc.size, tc.precision)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
