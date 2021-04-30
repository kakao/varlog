package mathutil

func MaxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func MinUint64(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func MaxUint64(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}
