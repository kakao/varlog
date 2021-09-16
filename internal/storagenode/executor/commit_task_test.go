package executor

import (
	"container/heap"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func TestCommitTaskBlockPool(t *testing.T) {
	for i := 0; i < 100; i++ {
		ctb := newCommitTask()
		require.Equal(t, types.InvalidVersion, ctb.version)
		require.Equal(t, types.InvalidGLSN, ctb.committedGLSNBegin)
		require.Equal(t, types.InvalidGLSN, ctb.committedGLSNEnd)
		ctb.version = 1
		ctb.committedGLSNBegin = 1
		ctb.committedGLSNEnd = 1
		ctb.release()
	}
}

type testCommitTaskHeap []*commitTask

func (b testCommitTaskHeap) Len() int { return len(b) }

func (b testCommitTaskHeap) Less(i, j int) bool { return b[i].version < b[j].version }

func (b testCommitTaskHeap) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

func (b *testCommitTaskHeap) Push(ct interface{}) { *b = append(*b, ct.(*commitTask)) }

func (b *testCommitTaskHeap) Pop() interface{} {
	heap := *b
	lastIdx := len(heap) - 1
	ret := heap[lastIdx]
	heap[lastIdx] = nil
	*b = heap[0:lastIdx]
	return ret
}

func BenchmarkCommitTaskBlockBatch(b *testing.B) {
	benchFunctions := []struct {
		name string
		f    func(*testing.B, []*commitTask)
	}{
		{
			name: "sort",
			f:    benchmarkCommitTaskBlockBatchSort,
		},
		{
			name: "heap",
			f:    benchmarkCommitTaskBlockBatchHeap,
		},
	}

	rand.Seed(time.Now().UnixNano())

	for _, benchFunc := range benchFunctions {
		b.Run(benchFunc.name, func(b *testing.B) {
			commitTasks := make([]*commitTask, 0, b.N)
			for i := 0; i < b.N; i++ {
				ct := &commitTask{}
				ct.version = types.Version(rand.Uint64())
				commitTasks = append(commitTasks, ct)
			}
			benchFunc.f(b, commitTasks)
		})
	}
}

func benchmarkCommitTaskBlockBatchSort(b *testing.B, commitTasks []*commitTask) {
	var ctbb testCommitTaskHeap
	ctbb = make([]*commitTask, 0, len(commitTasks))
	popped := make([]*commitTask, 0, len(commitTasks))

	b.ResetTimer()
	// append
	for _, ctb := range commitTasks {
		ctbb = append(ctbb, ctb)
	}
	require.Equal(b, len(ctbb), len(commitTasks))

	// sort
	sort.Slice(ctbb, ctbb.Less)

	// processing
	for _, ctb := range ctbb {
		popped = append(popped, ctb)
	}
	b.StopTimer()

	sorted := sort.SliceIsSorted(popped, testCommitTaskHeap(popped).Less)
	require.True(b, sorted)
}

func benchmarkCommitTaskBlockBatchHeap(b *testing.B, commitTasks []*commitTask) {
	tmp := make([]*commitTask, 0, len(commitTasks))
	ctbb := (*testCommitTaskHeap)(&tmp)
	popped := make([]*commitTask, 0, len(commitTasks))

	b.ResetTimer()
	// append
	for _, ctb := range commitTasks {
		heap.Push(ctbb, ctb)
	}
	require.Equal(b, len(*ctbb), len(commitTasks))

	// processing
	for ctbb.Len() > 0 {
		popped = append(popped, heap.Pop(ctbb).(*commitTask))
	}
	b.StopTimer()

	sorted := sort.SliceIsSorted(popped, testCommitTaskHeap(popped).Less)
	require.True(b, sorted)
}
