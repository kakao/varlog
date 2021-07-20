package executor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
)

func TestLogStreamContext(t *testing.T) {
	lsc := newLogStreamContext()

	globalHighWatermark, uncommittedLLSNBegin := lsc.reportCommitBase()
	require.Equal(t, types.InvalidGLSN, globalHighWatermark)
	require.Equal(t, types.MinLLSN, uncommittedLLSNBegin)
}
