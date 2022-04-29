package executorsmap

import (
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/logstream"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestExecutorsMapEmpty(t *testing.T) {
	emap := New(10)
	require.Zero(t, emap.Size())

	extor, ok := emap.Load(1, 1)
	require.Nil(t, extor)
	require.False(t, ok)

	numCalled := 0
	emap.Range(func(types.LogStreamID, types.TopicID, *logstream.Executor) bool {
		numCalled++
		return true
	})
	require.Zero(t, numCalled)
}

func TestExecutorsMapStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	emap := New(10)

	require.NoError(t, emap.Store(1, 1, nil))
	require.Equal(t, 1, emap.Size())
	loadedExtor, loaded := emap.Load(1, 1)
	require.True(t, loaded)
	require.Nil(t, loadedExtor)

	extor := new(logstream.Executor)
	require.NoError(t, emap.Store(1, 1, extor))
	require.Equal(t, 1, emap.Size())

	loadedExtor, loaded = emap.Load(1, 1)
	require.True(t, loaded)
	require.Equal(t, extor, loadedExtor)
}

func TestExecutorsMapLoadOrStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	emap := New(10)

	extor, loaded := emap.LoadOrStore(1, 1, nil)
	require.False(t, loaded)
	require.Nil(t, extor)

	require.Equal(t, 1, emap.Size())
	loadedExtor, loaded := emap.Load(1, 1)
	require.True(t, loaded)
	require.Nil(t, loadedExtor)

	actualExtor, loaded := emap.LoadOrStore(1, 1, new(logstream.Executor))
	require.True(t, loaded)
	require.Equal(t, loadedExtor, actualExtor)

	extor = new(logstream.Executor)
	require.NoError(t, emap.Store(1, 1, extor))
	require.Equal(t, 1, emap.Size())

	loadedExtor, loaded = emap.Load(1, 1)
	require.True(t, loaded)
	require.Equal(t, extor, loadedExtor)

	extor, loaded = emap.LoadAndDelete(1, 1)
	require.True(t, loaded)
	require.Equal(t, extor, loadedExtor)
}

func TestExecutorsMapLoadAndDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	emap := New(10)

	extor, loaded := emap.LoadAndDelete(1, 1)
	require.Nil(t, extor)
	require.False(t, loaded)
}

func TestExecutorsMapOverwrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	emap := New(10)

	extor := new(logstream.Executor)
	require.NoError(t, emap.Store(1, 1, extor))
	require.Equal(t, 1, emap.Size())

	require.Error(t, emap.Store(1, 1, extor))
	require.Error(t, emap.Store(1, 1, new(logstream.Executor)))
}

func TestExecutorsMapMultipleExecutors(t *testing.T) {
	const numExtors = 100

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	emap := New(10)
	extors := make([]*logstream.Executor, numExtors)
	lstpMap := make(map[types.LogStreamID]types.TopicID, numExtors)

	for i := 0; i < numExtors; i++ {
		require.Equal(t, i, emap.Size())
		extor := new(logstream.Executor)
		if i%2 == 0 {
			extor = nil
		}
		extors[i] = extor
		tpid := types.TopicID(rng.Int31())
		lsid := types.LogStreamID(i)
		lstpMap[lsid] = tpid
		require.NoError(t, emap.Store(tpid, lsid, extor))
		require.Equal(t, i+1, emap.Size())
	}

	require.Equal(t, numExtors, emap.Size())

	for i := 0; i < numExtors; i++ {
		lsid := types.LogStreamID(i)
		require.Contains(t, lstpMap, lsid)
		tpid := lstpMap[lsid]
		actual, loaded := emap.LoadOrStore(tpid, lsid, new(logstream.Executor))
		require.True(t, loaded)
		require.Equal(t, extors[i], actual)

		if i%2 != 0 {
			require.Error(t, emap.Store(tpid, lsid, new(logstream.Executor)))
		}
	}

	iteratedLSIDs := make([]types.LogStreamID, 0, numExtors)
	emap.Range(func(lsid types.LogStreamID, _ types.TopicID, extor *logstream.Executor) bool {
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
	emap.Range(func(types.LogStreamID, types.TopicID, *logstream.Executor) bool {
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
	lstpMap := make(map[types.LogStreamID]types.TopicID)

	var stored [numExtors]bool
	for i := 0; i < numExtors*10; i++ {
		pos := rand.Intn(numExtors)
		lsid := types.LogStreamID(pos)
		tpid := types.TopicID(rand.Int31())
		if _, ok := lstpMap[lsid]; ok {
			tpid = lstpMap[lsid]
		} else {
			lstpMap[lsid] = tpid
		}
		ok := !stored[lsid]
		if ok {
			require.NoError(t, emap.Store(tpid, lsid, new(logstream.Executor)))
			stored[lsid] = true
		} else {
			require.Error(t, emap.Store(tpid, lsid, new(logstream.Executor)))
		}
	}

	iteratedLSIDs := make([]types.LogStreamID, 0, numExtors)
	emap.Range(func(lsid types.LogStreamID, _ types.TopicID, _ *logstream.Executor) bool {
		iteratedLSIDs = append(iteratedLSIDs, lsid)
		return true
	})
	require.True(t, sort.SliceIsSorted(iteratedLSIDs, func(i, j int) bool {
		return iteratedLSIDs[i] < iteratedLSIDs[j]
	}))
}

func BenchmarkExecutorsMap(b *testing.B) {
	const (
		topicID   = types.TopicID(1)
		numExtors = 1e5
	)

	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	var muStdmap sync.RWMutex
	stdmap := make(map[types.LogStreamID]*logstream.Executor, numExtors)
	ordmap := New(numExtors)
	for i := 0; i < numExtors; i++ {
		lsid := types.LogStreamID(i)
		extor := new(logstream.Executor)
		require.NoError(b, ordmap.Store(topicID, lsid, extor))
		stdmap[lsid] = extor
	}

	loadIDs := make([]types.LogStreamID, numExtors*2)
	for i := 0; i < len(loadIDs); i++ {
		loadIDs[i] = types.LogStreamID(rand.Intn(numExtors * 2))
	}

	callback := func(lsid types.LogStreamID, extor *logstream.Executor) {
		_ = lsid
		_ = extor
	}

	mockExecutor := new(logstream.Executor)

	tcs := []struct {
		name      string
		benchfunc func(b *testing.B)
	}{
		{
			name: "stdmap_store",
			benchfunc: func(b *testing.B) {
				var mu sync.RWMutex
				stdmap := make(map[types.LogStreamID]*logstream.Executor, numExtors)

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
					_ = ordmap.Store(topicID, types.LogStreamID(i), mockExecutor)
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
					extor, ok := ordmap.Load(topicID, lsid)
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
					ordmap.Range(func(lsid types.LogStreamID, _ types.TopicID, extor *logstream.Executor) bool {
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
