package executorsmap

import (
	"fmt"
	"sort"
	"sync"

	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/pkg/types"
)

type executorSlot struct {
	extor *logstream.Executor
	id    logStreamTopicID
}

var nilSlot = executorSlot{}

// ExecutorsMap is a sorted map that maps pairs of LogStreamID and TopicID to executors.
// It is safe for concurrent use by multiple goroutines.
type ExecutorsMap struct {
	slots []executorSlot
	hash  map[logStreamTopicID]executorSlot
	mu    sync.RWMutex
}

// New returns a new ExecutorsMap with an initial size of initSize.
func New(initSize int) *ExecutorsMap {
	return &ExecutorsMap{
		slots: make([]executorSlot, 0, initSize),
		hash:  make(map[logStreamTopicID]executorSlot, initSize),
	}
}

// Load returns the executor stored in the map for a lsid, or nil if the executor is not present.
func (m *ExecutorsMap) Load(tpid types.TopicID, lsid types.LogStreamID) (extor *logstream.Executor, loaded bool) {
	id := packLogStreamTopicID(lsid, tpid)
	m.mu.RLock()
	slot, ok := m.fastLookup(id)
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return slot.extor, true
}

// Store stores the executor for a lsid. Setting nil as an executor is also possible. However,
// overwriting a non-nil executor is not possible.
func (m *ExecutorsMap) Store(tpid types.TopicID, lsid types.LogStreamID, extor *logstream.Executor) (err error) {
	id := packLogStreamTopicID(lsid, tpid)
	m.mu.Lock()

	slot, idx, ok := m.lookup(id)
	if ok {
		if slot.extor == nil {
			m.slots[idx].extor = extor
			slot.extor = extor
			m.hash[id] = slot
		} else {
			// Overwriting the executor is not allowed.
			err = fmt.Errorf("try to overwrite executor: %d", lsid)
		}
		m.mu.Unlock()
		return err
	}
	m.store(id, extor)

	m.mu.Unlock()
	return err
}

// LoadOrStore returns the existing executor for the lsid if present. If not, it stores the
// executor. The loaded result is true if the executor is loaded, otherwise, false.
func (m *ExecutorsMap) LoadOrStore(tpid types.TopicID, lsid types.LogStreamID, extor *logstream.Executor) (actual *logstream.Executor, loaded bool) {
	id := packLogStreamTopicID(lsid, tpid)
	m.mu.Lock()

	slot, ok := m.fastLookup(id)
	if ok {
		m.mu.Unlock()
		return slot.extor, true
	}
	m.store(id, extor)

	m.mu.Unlock()
	return extor, false
}

// LoadAndDelete deletes the executor for a lsid, and returns the old executor. The loaded result is
// true if the executor is loaded, otherwise, false.
func (m *ExecutorsMap) LoadAndDelete(tpid types.TopicID, lsid types.LogStreamID) (*logstream.Executor, bool) {
	id := packLogStreamTopicID(lsid, tpid)
	m.mu.Lock()

	slot, idx, ok := m.lookup(id)
	if !ok {
		m.mu.Unlock()
		return nil, false
	}
	m.delete(idx)
	delete(m.hash, id)

	m.mu.Unlock()
	return slot.extor, true
}

// Range calls f in order by LogStreamID for each executor in the map.
func (m *ExecutorsMap) Range(f func(types.LogStreamID, types.TopicID, *logstream.Executor) bool) {
	m.mu.RLock()
	for i := 0; i < len(m.slots); i++ {
		id := m.slots[i].id
		lsid, tpid := id.unpack()
		extor := m.slots[i].extor
		if !f(lsid, tpid, extor) {
			break
		}
	}
	m.mu.RUnlock()
}

// Size returns the number of elements in the ExecutorsMap.
func (m *ExecutorsMap) Size() int {
	m.mu.RLock()
	ret := len(m.slots)
	m.mu.RUnlock()
	return ret
}

func (m *ExecutorsMap) fastLookup(lsid logStreamTopicID) (extor executorSlot, ok bool) {
	extor, ok = m.hash[lsid]
	return
}

func (m *ExecutorsMap) lookup(lsid logStreamTopicID) (extor executorSlot, idx int, ok bool) {
	n := len(m.slots)
	idx = m.search(lsid)
	if idx < n && m.slots[idx].id == lsid {
		return m.slots[idx], idx, true
	}
	return nilSlot, n, false
}

func (m *ExecutorsMap) store(lsid logStreamTopicID, extor *logstream.Executor) {
	idx := m.search(lsid)
	slot := executorSlot{extor: extor, id: lsid}
	m.insert(idx, slot)
	m.hash[lsid] = slot
}

func (m *ExecutorsMap) search(lsid logStreamTopicID) int {
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
