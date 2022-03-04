package logstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/internal/batchlet"
)

func TestReplicateTaskPools(t *testing.T) {
	defer goleak.VerifyNone(t)

	for poolIdx, batchletLen := range batchlet.LengthClasses {
		rt := newReplicateTask(poolIdx)
		assert.Empty(t, rt.llsnList)
		assert.Equal(t, batchletLen, cap(rt.llsnList))
	}
}
