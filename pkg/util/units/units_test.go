package units

import (
	"testing"

	"github.com/stretchr/testify/require"
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

	size, err = FromByteSizeString("-1G")
	require.Error(t, err)

	size, err = FromByteSizeString("1G", 0, 1<<10)
	require.Error(t, err)

	size, err = FromByteSizeString("1KB", 1<<20)
	require.Error(t, err)
}

func TestToByteSizeString(t *testing.T) {
	const sizeInBytes = 1024
	size, err := FromByteSizeString(ToByteSizeString(sizeInBytes))
	require.NoError(t, err)
	require.EqualValues(t, sizeInBytes, size)
}
