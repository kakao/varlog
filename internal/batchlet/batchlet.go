package batchlet

var (
	LengthClasses = []int{
		1 << 4,
		1 << 6,
		1 << 8,
		1 << 10,
	}
)

func SelectLengthClass(size int) (idx, batchletLen int) {
	idx = 0
	for idx < len(LengthClasses)-1 {
		if size <= LengthClasses[idx] {
			break
		}
		idx++
	}
	return idx, LengthClasses[idx]
}
