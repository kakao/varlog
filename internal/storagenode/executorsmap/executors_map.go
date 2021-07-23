package executorsmap

import (
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/internal/storagenode/executor"
	"github.com/kakao/varlog/pkg/types"
)

type executorSlot struct {
	extor executor.Executor
	id    types.LogStreamID
}

var nilSlot = executorSlot{}

type ExecutorsMap struct {
	slots []executorSlot
	hash  map[types.LogStreamID]executorSlot
	mu    sync.RWMutex
}

func New(initSize int) *ExecutorsMap {
	return &ExecutorsMap{
		slots: make([]executorSlot, 0, initSize),
		hash:  make(map[types.LogStreamID]executorSlot, initSize),
	}
}

// Load returns the executor stored in the map for a lsid, or nil if the executor is not present.
func (m *ExecutorsMap) Load(lsid types.LogStreamID) (extor executor.Executor, loaded bool) {
	m.mu.RLock()
	slot, ok := m.fastLookup(lsid)
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return slot.extor, true
}

// Store stores the executor for a lsid. Setting nil as an executor is also possible. However,
// overwriting a non-nil executor is not possible.
func (m *ExecutorsMap) Store(lsid types.LogStreamID, extor executor.Executor) (err error) {
	m.mu.Lock()

	slot, idx, ok := m.lookup(lsid)
	if ok {
		if slot.extor == nil {
			m.slots[idx].extor = extor
			slot.extor = extor
			m.hash[lsid] = slot
		} else {
			// Overwriting the executor is not allowed.
			err = errors.Errorf("try to overwrite executor: %d", lsid)
		}
		m.mu.Unlock()
		return err
	}
	m.store(lsid, extor)

	m.mu.Unlock()
	return err
}

// LoadOrStore returns the existing executor for the lsid if present. If not, it stores the
// executor. The loaded result is true if the executor is loaded, otherwise, false.
func (m *ExecutorsMap) LoadOrStore(lsid types.LogStreamID, extor executor.Executor) (actual executor.Executor, loaded bool) {
	m.mu.Lock()

	slot, ok := m.fastLookup(lsid)
	if ok {
		m.mu.Unlock()
		return slot.extor, true
	}
	m.store(lsid, extor)

	m.mu.Unlock()
	return extor, false
}

// LoadAndDelete deletes the executor for a lsid, and returns the old executor. The loaded result is
// true if the executor is loaded, otherwise, false.
func (m *ExecutorsMap) LoadAndDelete(lsid types.LogStreamID) (executor.Executor, bool) {
	m.mu.Lock()

	slot, idx, ok := m.lookup(lsid)
	if !ok {
		m.mu.Unlock()
		return nil, false
	}
	m.delete(idx)
	delete(m.hash, lsid)

	m.mu.Unlock()
	return slot.extor, true
}

// Range calls f in order by LogStreamID for each executor in the map.
func (m *ExecutorsMap) Range(f func(types.LogStreamID, executor.Executor) bool) {
	m.mu.RLock()
	for i := 0; i < len(m.slots); i++ {
		lsid := m.slots[i].id
		extor := m.slots[i].extor
		if !f(lsid, extor) {
			break
		}
	}
	m.mu.RUnlock()
}

func (m *ExecutorsMap) Size() int {
	m.mu.RLock()
	ret := len(m.slots)
	m.mu.RUnlock()
	return ret
}

func (m *ExecutorsMap) fastLookup(lsid types.LogStreamID) (extor executorSlot, ok bool) {
	extor, ok = m.hash[lsid]
	return
}

func (m *ExecutorsMap) lookup(lsid types.LogStreamID) (extor executorSlot, idx int, ok bool) {
	n := len(m.slots)
	idx = m.search(lsid)
	if idx < n && m.slots[idx].id == lsid {
		return m.slots[idx], idx, true
	}
	return nilSlot, n, false
}

func (m *ExecutorsMap) store(lsid types.LogStreamID, extor executor.Executor) {
	idx := m.search(lsid)
	slot := executorSlot{extor: extor, id: lsid}
	m.insert(idx, slot)
	m.hash[lsid] = slot
}

func (m *ExecutorsMap) search(lsid types.LogStreamID) int {
	return sort.Search(len(m.slots), func(idx int) bool {
		return lsid <= m.slots[idx].id
	})
}

func (m *ExecutorsMap) insert(idx int, slot executorSlot) {
	if idx == len(m.slots) {
		m.slots = append(m.slots, slot)
		return
	}

	m.slots = append(m.slots[:idx+1], m.slots[idx:]...)
	m.slots[idx] = slot
}

func (m *ExecutorsMap) delete(idx int) {
	copy(m.slots[idx:], m.slots[idx+1:])
	m.slots[len(m.slots)-1].extor = nil
	m.slots = m.slots[:len(m.slots)-1]
}
