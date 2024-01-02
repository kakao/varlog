package units

import (
	"fmt"
	"math"
	"strconv"
	"strings"

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

// ToByteSizeString returns a string that represents the size defined by
// international standard IEC 80000-13.
func ToByteSizeString(size float64) string {
	return units.BytesSize(size)
}

// FromByteSizeString parses the argument sizeString representing byte size and
// returns the number of bytes or -1 if the sizeString cannot be parsable.
func FromByteSizeString(sizeString string, minMax ...int64) (size int64, err error) {
	sep := strings.LastIndexAny(sizeString, "01234567890. ")
	if sep == -1 {
		return -1, fmt.Errorf("invalid size: '%s'", sizeString)
	}

	sfx := sizeString[sep+1:]
	if strings.ContainsAny(sfx, "i") {
		size, err = units.RAMInBytes(sizeString)
	} else {
		size, err = units.FromHumanSize(sizeString)
	}
	if err != nil {
		return -1, err
	}

	min, max := int64(0), int64(math.MaxInt64)
	if len(minMax) > 0 {
		min = minMax[0]
	}
	if len(minMax) > 1 {
		max = minMax[1]
	}
	if size < min || size > max {
		return -1, fmt.Errorf("invalid size %s", sizeString)
	}
	return size, nil
}
