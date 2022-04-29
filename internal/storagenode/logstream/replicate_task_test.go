package logstream

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.daumkakao.com/varlog/varlog/internal/batchlet"
)

func TestReplicateTaskPools(t *testing.T) {
	for poolIdx, batchletLen := range batchlet.LengthClasses {
		rt := newReplicateTask(poolIdx)
		assert.Empty(t, rt.llsnList)
		assert.Equal(t, batchletLen, cap(rt.llsnList))
	}
}
