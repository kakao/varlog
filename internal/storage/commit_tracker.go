package storage

import (
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

type commitTrackerMap struct {
	mu sync.RWMutex
	m  map[types.LLSN]*appendTask
}

func newCommitTracker() commitTrackerMap {
	return commitTrackerMap{m: make(map[types.LLSN]*appendTask)}
}

func (trk *commitTrackerMap) get(llsn types.LLSN) (*appendTask, bool) {
	trk.mu.RLock()
	t, ok := trk.m[llsn]
	trk.mu.RUnlock()
	return t, ok
}

func (trk *commitTrackerMap) put(t *appendTask) {
	trk.mu.Lock()
	trk.m[t.llsn] = t
	trk.mu.Unlock()
}

func (trk *commitTrackerMap) del(llsn types.LLSN) {
	trk.mu.Lock()
	delete(trk.m, llsn)
	trk.mu.Unlock()
}
