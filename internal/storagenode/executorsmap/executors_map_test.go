package executorsmap

import (
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/executor"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func TestExecutorsMapEmpty(t *testing.T) {
	emap := New(10)
	require.Zero(t, emap.Size())

	extor, ok := emap.Load(1)
	require.Nil(t, extor)
	require.False(t, ok)

	numCalled := 0
	emap.Range(func(_ types.LogStreamID, _ executor.Executor) bool {
		numCalled++
		return true
	})
	require.Zero(t, numCalled)
}

func TestExecutorsMapStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	emap := New(10)

	require.NoError(t, emap.Store(1, nil))
	require.Equal(t, 1, emap.Size())
	loadedExtor, loaded := emap.Load(1)
	require.True(t, loaded)
	require.Nil(t, loadedExtor)

	extor := executor.NewMockExecutor(ctrl)
	require.NoError(t, emap.Store(1, extor))
	require.Equal(t, 1, emap.Size())

	loadedExtor, loaded = emap.Load(1)
	require.True(t, loaded)
	require.Equal(t, extor, loadedExtor)
}

func TestExecutorsMapLoadOrStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	emap := New(10)

	extor, loaded := emap.LoadOrStore(1, nil)
	require.False(t, loaded)
	require.Nil(t, extor)

	require.Equal(t, 1, emap.Size())
	loadedExtor, loaded := emap.Load(1)
	require.True(t, loaded)
	require.Nil(t, loadedExtor)

	actualExtor, loaded := emap.LoadOrStore(1, executor.NewMockExecutor(ctrl))
	require.True(t, loaded)
	require.Equal(t, loadedExtor, actualExtor)

	extor = executor.NewMockExecutor(ctrl)
	require.NoError(t, emap.Store(1, extor))
	require.Equal(t, 1, emap.Size())

	loadedExtor, loaded = emap.Load(1)
	require.True(t, loaded)
	require.Equal(t, extor, loadedExtor)

	extor, loaded = emap.LoadAndDelete(1)
	require.True(t, loaded)
	require.Equal(t, extor, loadedExtor)
}

func TestExecutorsMapLoadAndDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	emap := New(10)

	extor, loaded := emap.LoadAndDelete(1)
	require.Nil(t, extor)
	require.False(t, loaded)
}

func TestExecutorsMapOverwrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	emap := New(10)

	extor := executor.NewMockExecutor(ctrl)
	require.NoError(t, emap.Store(1, extor))
	require.Equal(t, 1, emap.Size())

	require.Error(t, emap.Store(1, extor))
	require.Error(t, emap.Store(1, executor.NewMockExecutor(ctrl)))
}

func TestExecutorsMapMultipleExecutors(t *testing.T) {
	const numExtors = 100

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	emap := New(10)
	extors := make([]executor.Executor, numExtors)

	for i := 0; i < numExtors; i++ {
		require.Equal(t, i, emap.Size())
		var extor executor.Executor = executor.NewMockExecutor(ctrl)
		if i%2 == 0 {
			extor = nil
		}
		extors[i] = extor
		emap.Store(types.LogStreamID(i), extor)
		require.Equal(t, i+1, emap.Size())
	}

	require.Equal(t, numExtors, emap.Size())

	for i := 0; i < numExtors; i++ {
		actual, loaded := emap.LoadOrStore(types.LogStreamID(i), executor.NewMockExecutor(ctrl))
		require.True(t, loaded)
		require.Equal(t, extors[i], actual)

		if i%2 != 0 {
			require.Error(t, emap.Store(types.LogStreamID(1), executor.NewMockExecutor(ctrl)))
		}
	}

	iteratedLSIDs := make([]types.LogStreamID, 0, numExtors)
	emap.Range(func(lsid types.LogStreamID, extor executor.Executor) bool {
		if lsid%2 == 0 {
			require.Nil(t, extor)
		}
		iteratedLSIDs = append(iteratedLSIDs, lsid)
		require.Equal(t, extors[int(lsid)], extor)
		return true
	})
	require.True(t, sort.SliceIsSorted(iteratedLSIDs, func(i, j int) bool {
		return iteratedLSIDs[i] < iteratedLSIDs[j]
	}))

	numCalled := 0
	emap.Range(func(types.LogStreamID, executor.Executor) bool {
		numCalled++
		return false
	})
	require.Equal(t, 1, numCalled)
}

func TestExecutorsMapOrdred(t *testing.T) {
	const numExtors = 1000

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rand.Seed(time.Now().UnixNano())

	emap := New(numExtors)

	var stored [numExtors]bool
	for i := 0; i < numExtors*10; i++ {
		lsid := rand.Intn(numExtors)
		ok := !stored[lsid]
		if ok {
			require.NoError(t, emap.Store(types.LogStreamID(lsid), executor.NewMockExecutor(ctrl)))
			stored[lsid] = true
		} else {
			require.Error(t, emap.Store(types.LogStreamID(lsid), executor.NewMockExecutor(ctrl)))
		}
	}

	iteratedLSIDs := make([]types.LogStreamID, 0, numExtors)
	emap.Range(func(lsid types.LogStreamID, extor executor.Executor) bool {
		iteratedLSIDs = append(iteratedLSIDs, lsid)
		return true
	})
	require.True(t, sort.SliceIsSorted(iteratedLSIDs, func(i, j int) bool {
		return iteratedLSIDs[i] < iteratedLSIDs[j]
	}))
}

func BenchmarkExecutorsMap(b *testing.B) {
	const (
		numExtors = 1e5
		initSize  = 128
	)

	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	var muStdmap sync.RWMutex
	stdmap := make(map[types.LogStreamID]executor.Executor, numExtors)
	ordmap := New(numExtors)
	for i := 0; i < numExtors; i++ {
		lsid := types.LogStreamID(i)
		extor := executor.NewMockExecutor(ctrl)
		require.NoError(b, ordmap.Store(lsid, extor))
		stdmap[lsid] = extor
	}

	loadIDs := make([]types.LogStreamID, numExtors*2)
	for i := 0; i < len(loadIDs); i++ {
		loadIDs[i] = types.LogStreamID(rand.Intn(numExtors * 2))
	}

	callback := func(lsid types.LogStreamID, extor executor.Executor) {
		_ = lsid
		_ = extor
	}

	mockExecutor := executor.NewMockExecutor(ctrl)

	tcs := []struct {
		name      string
		benchfunc func(b *testing.B)
	}{
		{
			name: "stdmap_store",
			benchfunc: func(b *testing.B) {
				var mu sync.RWMutex
				stdmap := make(map[types.LogStreamID]executor.Executor, numExtors)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					mu.Lock()
					stdmap[types.LogStreamID(i)] = mockExecutor
					mu.Unlock()
				}
			},
		},
		{
			name: "ordmap_store",
			benchfunc: func(b *testing.B) {
				ordmap := New(numExtors)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					ordmap.Store(types.LogStreamID(i), mockExecutor)
				}
			},
		},
		{
			name: "stdmap_load",
			benchfunc: func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					muStdmap.RLock()
					lsid := loadIDs[i%len(loadIDs)]
					extor, ok := stdmap[lsid]
					if ok {
						callback(lsid, extor)
					}
					muStdmap.RUnlock()
				}
			},
		},
		{
			name: "ordmap_load",
			benchfunc: func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					lsid := loadIDs[i%len(loadIDs)]
					extor, ok := ordmap.Load(lsid)
					if ok {
						callback(lsid, extor)
					}
				}
			},
		},
		{
			name: "stdmap_range",
			benchfunc: func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					muStdmap.RLock()
					for lsid, extor := range stdmap {
						callback(lsid, extor)
					}
					muStdmap.RUnlock()
				}
			},
		},
		{
			name: "ordmap_range",
			benchfunc: func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					ordmap.Range(func(lsid types.LogStreamID, extor executor.Executor) bool {
						callback(lsid, extor)
						return true
					})
				}
			},
		},
	}

	for i := range tcs {
		tc := tcs[i]
		b.Run(tc.name, func(b *testing.B) {
			tc.benchfunc(b)
		})
	}
}
