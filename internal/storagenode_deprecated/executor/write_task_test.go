package executor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func TestTaskBlockPool(t *testing.T) {
	for i := 0; i < 100; i++ {
		wt := newWriteTaskInternal(&taskWaitGroup{}, nil)
		require.Equal(t, types.InvalidLLSN, wt.llsn)
		wt.llsn = 1
		wt.release()
	}
}
