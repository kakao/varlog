package units

import (
	"fmt"
	"math"

	"github.com/docker/go-units"
)

func ToByteSizeString(size float64) string {
	return units.BytesSize(size)
}

func FromByteSizeString(sizeString string, minMax ...int64) (int64, error) {
	size, err := units.RAMInBytes(sizeString)
	if err != nil {
		return size, err
	}
	min, max := int64(0), int64(math.MaxInt64)
	if len(minMax) > 0 {
		min = minMax[0]
	}
	if len(minMax) > 1 {
		max = minMax[1]
	}
	if size < min || size > max {
		return size, fmt.Errorf("invalid size %s", sizeString)
	}
	return size, nil
}
