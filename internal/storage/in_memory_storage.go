package storage

import (
	"sort"
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
)

type imLogEntry struct {
	data      []byte
	llsn      types.LLSN
	glsn      types.GLSN
	committed bool
}

type imLogEntrySorter []imLogEntry

func (s imLogEntrySorter) Len() int {
	return len(s)
}

func (s imLogEntrySorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s imLogEntrySorter) Less(i, j int) bool {
	return s[i].glsn < s[j].glsn
}

type InMemoryStorage struct {
	nextLLSN          types.LLSN
	committedEndIdx   uint64
	uncommittedEndIdx uint64
	entries           []imLogEntry
	m                 sync.RWMutex
}

func NewInMemoryStorage() Storage {
	return &InMemoryStorage{}
}

func (s *InMemoryStorage) Read(glsn types.GLSN) ([]byte, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	s.assert()
	defer s.assert()

	committedEntries := s.committedEntries()
	if len(committedEntries) == 0 {
		return nil, varlog.ErrNoEntry
	}

	if committedEntries[0].glsn > glsn {
		return nil, varlog.ErrNoEntry
	}
	if committedEntries[len(committedEntries)-1].glsn < glsn {
		return nil, varlog.ErrNoEntry
	}
	i := sort.Search(len(committedEntries), func(i int) bool {
		return committedEntries[i].glsn >= glsn
	})
	if i < len(committedEntries) && committedEntries[i].glsn == glsn {
		return committedEntries[i].data, nil
	}

	return nil, varlog.ErrNoEntry
}

func (s *InMemoryStorage) Scan(glsn types.GLSN) (Scanner, error) {
	s.assert()
	defer s.assert()
	panic("not yet implemented")
}

func (s *InMemoryStorage) Write(llsn types.LLSN, data []byte) error {
	s.m.Lock()
	defer s.m.Unlock()

	s.assert()
	defer s.assert()

	if s.nextLLSN != llsn {
		return varlog.ErrInvalid
	}
	entry := imLogEntry{
		data: data,
		llsn: llsn,
	}
	s.entries = append(s.entries, entry)
	s.nextLLSN++
	s.uncommittedEndIdx++
	return nil
}

func (s *InMemoryStorage) Commit(llsn types.LLSN, glsn types.GLSN) error {
	s.m.Lock()
	defer s.m.Unlock()

	s.assert()
	defer s.assert()

	entry := &s.entries[s.committedEndIdx]
	if entry.llsn != llsn {
		panic("broken storage")
	}

	entry.glsn = glsn
	entry.committed = true
	s.committedEndIdx++
	return nil
}

func (s *InMemoryStorage) Delete(glsn types.GLSN) (uint64, error) {
	s.m.Lock()
	defer s.m.Unlock()

	s.assert()
	defer s.assert()

	if s.committedEndIdx == 0 {
		return 0, nil
	}
	if s.entries[0].glsn > glsn {
		return 0, nil
	}
	committedEntries := s.committedEntries()
	i := sort.Search(len(committedEntries), func(i int) bool {
		return committedEntries[i].glsn > glsn
	})
	numTrimmed := uint64(i)
	copy(s.entries, s.entries[numTrimmed:])
	s.committedEndIdx -= numTrimmed
	s.uncommittedEndIdx -= numTrimmed
	return numTrimmed, nil
}

func (s *InMemoryStorage) assert() {
	if s.committedEndIdx > s.uncommittedEndIdx {
		goto out
	}
	if s.committedEndIdx > 0 && !s.entries[s.committedEndIdx-1].committed {
		goto out
	}
	return
out:
	panic("bad storage")
}

func (s *InMemoryStorage) committedEntries() []imLogEntry {
	return s.entries[:s.committedEndIdx]
}

func (s *InMemoryStorage) uncommittedEntries() []imLogEntry {
	return s.entries[s.committedEndIdx:s.uncommittedEndIdx]
}
