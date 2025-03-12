package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func BenchmarkStorage_WriteBatch(b *testing.B) {
	for _, batchLen := range []int{10, 100, 1000} {
		batchLen := batchLen
		for _, dataLen := range []int{10, 100, 1000} {
			dataLen := dataLen
			data := make([]byte, dataLen)
			for i := 0; i < dataLen; i++ {
				data[i] = '.'
			}

			name := fmt.Sprintf("batchLen=%d_dataLen=%d", batchLen, dataLen)
			b.Run(name, func(b *testing.B) {
				stg := TestNewStorage(b,
					WithDataDBOptions(
						WithSync(false), // Use only in mac since sync is too slow in mac os.
						WithL0CompactionThreshold(2),
						WithL0StopWritesThreshold(1000),
						WithLBaseMaxBytes(64<<20),
						WithMaxOpenFiles(16384),
						WithMemTableSize(64<<20),
						WithMemTableStopWritesThreshold(4),
						WithMaxConcurrentCompaction(3),
					),
				)
				defer func() {
					assert.NoError(b, stg.Close())
				}()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					wb := stg.NewWriteBatch()
					for j := 0; j < batchLen; j++ {
						_ = wb.Set(types.LLSN(i), data)
					}
					if err := wb.Apply(); err != nil {
						b.Error(err)
					}
					if err := wb.Close(); err != nil {
						b.Error(err)
					}
				}
			})
		}
	}
}

func BenchmarkStorage_ScanWithGLSN(b *testing.B) {
	numLogsList := []int{
		1, 10, 100, 1000,
	}

	for _, numLogs := range numLogsList {
		b.Run(fmt.Sprintf("numLogs=%d", numLogs), func(b *testing.B) {
			stg := TestNewStorage(b,
				WithDataDBOptions(
					WithSync(false), // Use only in mac since sync is too slow in mac os.
					WithL0CompactionThreshold(2),
					WithL0StopWritesThreshold(1000),
					WithLBaseMaxBytes(64<<20),
					WithMaxOpenFiles(16384),
					WithMemTableSize(64<<20),
					WithMemTableStopWritesThreshold(4),
					WithMaxConcurrentCompaction(3),
				),
				WithCommitDBOptions(
					WithL0CompactionThreshold(2),
					WithL0StopWritesThreshold(1000),
					WithLBaseMaxBytes(64<<20),
					WithMaxOpenFiles(16384),
					WithMemTableSize(8<<20),
					WithMemTableStopWritesThreshold(4),
					WithMaxConcurrentCompaction(3),
				),
			)
			b.Cleanup(func() {
				assert.NoError(b, stg.Close())
			})

			for i := range numLogs {
				llsn := types.LLSN(i + 1)
				glsn := types.GLSN(i + 1)
				data := []byte(fmt.Sprintf("log entry %d", i+1))
				TestAppendLogEntryWithoutCommitContext(b, stg, llsn, glsn, data)
			}
			TestSetCommitContext(b, stg, CommitContext{
				Version:            1,
				HighWatermark:      types.GLSN(numLogs),
				CommittedGLSNBegin: types.MinGLSN,
				CommittedGLSNEnd:   types.GLSN(numLogs + 1),
				CommittedLLSNBegin: types.MinLLSN,
			})

			b.ResetTimer()

			var logEntry varlogpb.LogEntry
			for range b.N {
				scanner, err := stg.NewScanner(WithGLSN(types.MinGLSN, types.GLSN(numLogs+1)))
				require.NoError(b, err)
				for scanner.Valid() {
					logEntry, err = scanner.Value()
					require.NoError(b, err)
					scanner.Next()
				}
				err = scanner.Close()
				require.NoError(b, err)
			}
			_ = logEntry
		})
	}
}
