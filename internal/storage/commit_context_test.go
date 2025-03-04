package storage

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/kakao/varlog/pkg/types"
)

func TestCommitContext_MarshalLogObject(t *testing.T) {
	cc := CommitContext{
		Version:            types.Version(rand.Uint64()),
		HighWatermark:      types.GLSN(rand.Uint64()),
		CommittedGLSNBegin: types.GLSN(rand.Uint64()),
		CommittedGLSNEnd:   types.GLSN(rand.Uint64()),
		CommittedLLSNBegin: types.LLSN(rand.Uint64()),
	}

	logger := zaptest.NewLogger(t)
	t.Cleanup(func() {
		_ = logger.Sync()
	})

	logger.Info("test", zap.Object("commitContext", cc))
	require.Panics(t, func() {
		logger.Info("test", zap.Object("commitContext", nil))
	})
}
