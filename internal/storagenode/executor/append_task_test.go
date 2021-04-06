package executor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
)

func TestTaskBlockPool(t *testing.T) {
	for i := 0; i < 100; i++ {
		tb := newAppendTask()
		require.Equal(t, types.InvalidLLSN, tb.llsn)
		require.Equal(t, types.InvalidGLSN, tb.glsn)
		tb.llsn = 1
		tb.glsn = 1
		tb.release()
	}
}

func TestTaskBlockBatchPool(t *testing.T) {
	const batchSize = 32
	pool := newAppendTaskBatchPool(32)
	for i := 0; i < 100; i++ {
		tbb := newAppendTaskBatch(pool)
		require.Equal(t, batchSize, cap(tbb.batch))
		require.Len(t, tbb.batch, 0)

		tbb.batch = append(tbb.batch, &appendTask{})
		tbb.batch = append(tbb.batch, &appendTask{})
		tbb.batch = append(tbb.batch, &appendTask{})
		tbb.batch = append(tbb.batch, &appendTask{})

		tbb.release()
	}
}

func TestTaskBlockPoolAndBatchPool(t *testing.T) {
	const batchSize = 32

	pool := newAppendTaskBatchPool(batchSize)
	for i := 0; i < 100; i++ {
		tbb := newAppendTaskBatch(pool)
		require.Equal(t, batchSize, cap(tbb.batch))
		require.Len(t, tbb.batch, 0)

		tbb.batch = append(tbb.batch, newAppendTask())
		tbb.batch = append(tbb.batch, newAppendTask())
		tbb.batch = append(tbb.batch, newAppendTask())
		tbb.batch = append(tbb.batch, newAppendTask())

		for _, batch := range tbb.batch {
			batch.release()
		}

		tbb.release()
	}
}
