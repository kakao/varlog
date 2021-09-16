package executor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
)

func TestLogStreamContext(t *testing.T) {
	lsc := newLogStreamContext()

	version, highWatermark, uncommittedLLSNBegin := lsc.reportCommitBase()
	require.Equal(t, types.InvalidVersion, version)
	require.Equal(t, types.MinGLSN, highWatermark)
	require.Equal(t, types.MinLLSN, uncommittedLLSNBegin)
}
