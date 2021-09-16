package executor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func TestCommitWaitQueue(t *testing.T) {
	const n = 10

	cwq := newCommitWaitQueue()
	require.Zero(t, cwq.size())
	iter := cwq.peekIterator()
	require.False(t, iter.valid())
	require.False(t, iter.next())
	require.Nil(t, iter.task())

	for i := 0; i < n; i++ {
		require.Equal(t, i, cwq.size())
		cwq.push(&commitWaitTask{llsn: types.LLSN(i + 1)})
		require.Equal(t, i+1, cwq.size())
	}

	iter = cwq.peekIterator()
	for i := 1; i < n; i++ {
		require.True(t, iter.valid())
		require.Equal(t, types.LLSN(i), iter.task().llsn)
		require.True(t, iter.next())
	}

	require.True(t, iter.valid())
	require.Equal(t, types.LLSN(n), iter.task().llsn)
	require.False(t, iter.next())
	require.False(t, iter.valid())
}
