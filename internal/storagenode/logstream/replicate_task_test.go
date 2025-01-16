package logstream

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kakao/varlog/internal/batchlet"
)

func TestReplicateTaskPools(t *testing.T) {
	const repeatCount = 1000

	for range repeatCount {
		for poolIdx, batchletLen := range batchlet.LengthClasses {
			rt1 := newReplicateTaskDeprecated(poolIdx)
			assert.Empty(t, rt1.llsnList)
			assert.Equal(t, batchletLen, cap(rt1.llsnList))

			rt2 := newReplicateTask(batchletLen)
			assert.Empty(t, rt2.llsnList)
			assert.GreaterOrEqual(t, cap(rt2.llsnList), batchletLen)

			rt1.releaseDeprecated()
			rt2.release()
		}
	}
}
