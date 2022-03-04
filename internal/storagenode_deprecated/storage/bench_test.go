package storage

import (
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

func BenchmarkStorageReadAt(b *testing.B) {
	const numLogs = 1e5
	prepare := func(b *testing.B, strg Storage, numLogs int) {
		for i := 0; i < numLogs; i++ {
			llsn := types.LLSN(i + 1)
			glsn := types.GLSN(llsn * 3)
			wb := strg.NewWriteBatch()
			require.NoError(b, wb.Put(llsn, nil))
			require.NoError(b, wb.Apply())
			require.NoError(b, wb.Close())

			cb, err := strg.NewCommitBatch(CommitContext{
				Version:            types.Version(i + 1),
				HighWatermark:      glsn,
				CommittedGLSNBegin: glsn,
				CommittedGLSNEnd:   glsn + 1,
				CommittedLLSNBegin: llsn,
			})
			require.NoError(b, err)
			require.NoError(b, cb.Put(llsn, glsn))
			require.NoError(b, cb.Apply())
			require.NoError(b, cb.Close())
		}
	}

	testCases := []struct {
		name string
		f    func(b *testing.B, ps *pebbleStorage, llsn types.LLSN)
	}{
		{name: "readyByScan", f: readByScan},
		{name: "readyBySeek", f: readBySeek},
	}

	for i := range testCases {
		tc := testCases[i]
		b.Run(tc.name, func(b *testing.B) {
			rng := rand.New(rand.NewSource(time.Now().Unix()))

			strg, err := NewStorage(
				WithName("pebble"),
				WithPath(b.TempDir()),
				WithLogger(zap.NewNop()),
				WithoutWriteSync(),
				WithoutCommitSync(),
			)
			require.NoError(b, err)
			defer func() {
				require.NoError(b, strg.Close())
			}()

			prepare(b, strg, numLogs)

			ps := strg.(*pebbleStorage)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				llsn := types.LLSN(rng.Intn(numLogs) + 1)
				tc.f(b, ps, llsn)
			}
			b.StopTimer()
		})
	}
}

func readByScan(b *testing.B, ps *pebbleStorage, llsn types.LLSN) {
	iter := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitKeyPrefix},
		UpperBound: []byte{commitKeySentinelPrefix},
	})
	defer func() {
		_ = iter.Close()
	}()

	iter.First()
	for iter.Valid() && decodeCommitValue(iter.Value()) <= llsn {
		if decodeCommitValue(iter.Value()) == llsn {
			glsn := decodeCommitKey(iter.Key())
			_, err := ps.Read(glsn)
			if err == nil {
				return
			}
			require.NoErrorf(b, err, "no such log: llsn=%d", llsn)
		}
		iter.Next()
	}
	require.Fail(b, "no such log: llsn=%d", llsn)
}

func readBySeek(b *testing.B, ps *pebbleStorage, llsn types.LLSN) {
	_, err := ps.ReadAt(llsn)
	if err == nil {
		return
	}
	require.NoErrorf(b, err, "no such log: llsn=%d", llsn)
}
