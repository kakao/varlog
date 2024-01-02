package units

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestFromByteSizeString(t *testing.T) {
	const SI = ""
	const IEC = "i"

	tcs := []struct {
		input string
		want  int64
	}{
		{input: "", want: -1},
		{input: "0", want: 0},
		{input: "1", want: 1},
		{input: "1B", want: 1},
		{input: "-1", want: -1},
		{input: "1Gi", want: -1},
	}

	for i, symbol := range []string{"K", "M", "G", "T", "P"} {
		for _, standard := range []string{SI, IEC} {
			var base float64
			if standard == SI {
				base = 1000
			} else {
				base = 1024
			}
			want := int64(math.Pow(base, float64(i+1)))

			for _, symbol := range []string{symbol, strings.ToLower(symbol)} {
				for _, standard := range []string{standard, strings.ToLower(standard)} {
					for _, b := range []string{"b", "B"} {
						input := fmt.Sprintf("1%s%s%s", symbol, standard, b)
						tcs = append(tcs, struct {
							input string
							want  int64
						}{
							input: input,
							want:  want,
						})
					}
				}
			}
		}
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			got, err := FromByteSizeString(tc.input)
			if tc.want < 0 {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
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
