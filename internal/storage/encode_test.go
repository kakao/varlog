package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeCommitContextUnsafe(t *testing.T) {
	expected := CommitContext{
		Version:            1,
		HighWatermark:      2,
		CommittedGLSNBegin: 3,
		CommittedGLSNEnd:   4,
		CommittedLLSNBegin: 5,
	}
	buf := encodeCommitContextUnsafe(&expected)
	actual := decodeCommitContextUnsafe(buf)
	require.Equal(t, expected, actual)
}

func BenchmarkCommitContext_Decode(b *testing.B) {
	tcs := []struct {
		name   string
		benchf func(*testing.B, CommitContext)
	}{
		{
			name: "Safe",
			benchf: func(b *testing.B, cc CommitContext) {
				buf := make([]byte, commitContextLength)
				buf = encodeCommitContext(cc, buf)
				for i := 0; i < b.N; i++ {
					_ = decodeCommitContext(buf)
				}
			},
		},
		{
			name: "Unsafe",
			benchf: func(b *testing.B, cc CommitContext) {
				buf := encodeCommitContextUnsafe(&cc)
				for i := 0; i < b.N; i++ {
					_ = decodeCommitContextUnsafe(buf)
				}
			},
		},
	}

	for _, tc := range tcs {
		b.Run(tc.name, func(b *testing.B) {
			cc := CommitContext{
				Version:            1,
				HighWatermark:      2,
				CommittedGLSNBegin: 3,
				CommittedGLSNEnd:   4,
				CommittedLLSNBegin: 5,
			}
			tc.benchf(b, cc)
		})
	}
}
