package units

import (
	"fmt"
	"math"
	"strconv"

	"github.com/docker/go-units"
)

const humanSizeBase = 1000.0

var (
	siUnit = []string{"", "k", "M", "G", "T", "P", "E", "Z", "Y"}
)

func ToHumanSizeStringWithoutUnit(size float64, precision int) string {
	fmt := "%." + strconv.Itoa(precision) + "g%s"
	return units.CustomSize(fmt, size, humanSizeBase, siUnit)
}

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
