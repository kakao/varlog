package logstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReplicateTaskPools(t *testing.T) {
	const repeatCount = 1000
	lengthList := []int{
		1 << 4,
		1 << 6,
		1 << 8,
		1 << 10,
	}

	for range repeatCount {
		for _, length := range lengthList {
			rt2 := newReplicateTask(length)
			assert.Empty(t, rt2.llsnList)
			assert.GreaterOrEqual(t, cap(rt2.llsnList), length)
			rt2.release()
		}
	}
}
