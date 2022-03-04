package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.daumkakao.com/varlog/varlog/pkg/types"
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
					WithoutSync(),
					WithL0CompactionThreshold(2),
					WithL0StopWritesThreshold(1000),
					WithLBaseMaxBytes(64<<20),
					WithMaxOpenFiles(16384),
					WithMemTableSize(64<<20),
					WithMemTableStopWritesThreshold(4),
					WithMaxConcurrentCompaction(3),
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
