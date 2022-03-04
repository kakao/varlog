package executor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func TestCommitTaskBlockPool(t *testing.T) {
	for i := 0; i < 100; i++ {
		ctb := newCommitTask()
		require.Equal(t, types.InvalidVersion, ctb.version)
		require.Equal(t, types.InvalidGLSN, ctb.committedGLSNBegin)
		require.Equal(t, types.InvalidGLSN, ctb.committedGLSNEnd)
		ctb.version = 1
		ctb.committedGLSNBegin = 1
		ctb.committedGLSNEnd = 1
		ctb.release()
	}
}
