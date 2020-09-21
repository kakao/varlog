package storage

import (
	"fmt"
	"sort"
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
)

type writtenEntry struct {
	llsn types.LLSN
	data []byte
}

type committedEntry struct {
	llsn types.LLSN
	glsn types.GLSN
}

type InMemoryStorage struct {
	muWritten sync.RWMutex
	written   []writtenEntry

	muCommitted sync.RWMutex
	committed   []committedEntry
}

type InMemoryScanner struct {
	cursor  types.LLSN
	storage *InMemoryStorage
}

func (s *InMemoryScanner) Next() (varlog.LogEntry, error) {
	s.storage.muCommitted.RLock()
	defer s.storage.muCommitted.RUnlock()
	s.storage.muWritten.RLock()
	defer s.storage.muWritten.RUnlock()

	if len(s.storage.written) == 0 || len(s.storage.committed) == 0 {
		return varlog.LogEntry{}, varlog.ErrNoEntry
	}

	idx := uint64(s.cursor - s.storage.written[0].llsn)
	if idx >= uint64(len(s.storage.committed)) {
		return varlog.LogEntry{}, varlog.ErrNoEntry
	}

	went := s.storage.written[idx]
	cent := s.storage.committed[idx]
	if went.llsn != cent.llsn {
		// TODO (jun): storage is broken
		return varlog.LogEntry{}, varlog.ErrInternal
	}
	ret := varlog.LogEntry{
		LLSN: cent.llsn,
		GLSN: cent.glsn,
		Data: went.data,
	}
	s.cursor++
	return ret, nil
}

func NewInMemoryStorage() Storage {
	return &InMemoryStorage{}
}

func (s *InMemoryStorage) searchCommittedEntry(glsn types.GLSN) (int, committedEntry, error) {
	i := sort.Search(len(s.committed), func(i int) bool { return s.committed[i].glsn >= glsn })
	if i >= len(s.committed) {
		return i, committedEntry{}, varlog.ErrNoEntry
	}
	if s.committed[i].glsn == glsn {
		return i, s.committed[i], nil
	}
	return i, s.committed[i], varlog.ErrNoEntry
}

func (s *InMemoryStorage) Read(glsn types.GLSN) (varlog.LogEntry, error) {
	s.assert()
	defer s.assert()

	s.muCommitted.RLock()
	defer s.muCommitted.RUnlock()
	if len(s.committed) == 0 {
		return varlog.InvalidLogEntry, varlog.ErrNoEntry
	}

	first := s.committed[0]
	last := s.committed[len(s.committed)-1]
	if first.glsn > glsn || last.glsn < glsn {
		return varlog.InvalidLogEntry, varlog.ErrNoEntry
	}

	i, _, err := s.searchCommittedEntry(glsn)
	if err != nil {
		return varlog.InvalidLogEntry, varlog.ErrNoEntry
	}
	// NB: The LLSN of the first entry of written and committed should be same.
	// NB: committedEntry[i] and writtenEntry[i] are the same log entry.
	s.muWritten.RLock()
	defer s.muWritten.RUnlock()
	went := s.written[i]
	return varlog.LogEntry{
		GLSN: glsn,
		LLSN: went.llsn,
		Data: went.data,
	}, nil
}

func (s *InMemoryStorage) Scan(glsn types.GLSN) (Scanner, error) {
	s.assert()
	defer s.assert()

	s.muCommitted.RLock()
	defer s.muCommitted.RUnlock()

	i, cent, _ := s.searchCommittedEntry(glsn)
	if i >= len(s.committed) {
		return nil, varlog.ErrNoEntry
	}
	scanner := &InMemoryScanner{
		cursor:  cent.llsn,
		storage: s,
	}
	return scanner, nil
}

func (s *InMemoryStorage) Write(llsn types.LLSN, data []byte) error {
	s.assert()
	defer s.assert()

	s.muWritten.Lock()
	defer s.muWritten.Unlock()

	if len(s.written) > 0 && s.written[len(s.written)-1].llsn+1 != llsn {
		return varlog.ErrInvalid
	}

	s.written = append(s.written, writtenEntry{llsn: llsn, data: data})
	return nil
}

func (s *InMemoryStorage) searchWrittenEnry(llsn types.LLSN) (writtenEntry, error) {
	i := sort.Search(len(s.written), func(i int) bool { return s.written[i].llsn >= llsn })
	if i < len(s.written) && s.written[i].llsn == llsn {
		return s.written[i], nil
	}
	return writtenEntry{}, varlog.ErrNoEntry
}

func (s *InMemoryStorage) Commit(llsn types.LLSN, glsn types.GLSN) error {
	s.assert()
	defer s.assert()

	s.muWritten.RLock()
	_, err := s.searchWrittenEnry(llsn)
	s.muWritten.RUnlock()
	if err != nil {
		return err
	}

	s.muCommitted.Lock()
	defer s.muCommitted.Unlock()

	if len(s.committed) > 0 {
		last := s.committed[len(s.committed)-1]
		if last.llsn+1 != llsn || last.glsn >= glsn {
			return varlog.ErrInvalid
		}
	}
	s.committed = append(s.committed, committedEntry{llsn: llsn, glsn: glsn})
	return nil
}

func (s *InMemoryStorage) DeleteCommitted(glsn types.GLSN) error {
	s.assert()
	defer s.assert()

	s.muCommitted.Lock()
	defer s.muCommitted.Unlock()

	if len(s.committed) == 0 {
		// no committed entries
		return nil
	}
	first := s.committed[0]
	if glsn < first.glsn {
		// no entries to delete
		return nil
	}

	i, _, err := s.searchCommittedEntry(glsn)
	if err == nil {
		i++
	}
	s.committed = s.committed[i:]
	s.muWritten.Lock()
	s.written = s.written[i:]
	s.muWritten.Unlock()
	return nil
}

func (s *InMemoryStorage) DeleteUncommitted(llsn types.LLSN) error {
	s.muCommitted.Lock()
	defer s.muCommitted.Unlock()
	s.muWritten.Lock()
	defer s.muWritten.Unlock()
	i := sort.Search(len(s.written), func(i int) bool { return s.written[i].llsn >= llsn })
	if i >= len(s.written) {
		// no such entry, but no problem
		return nil
	}
	if s.written[i].llsn != llsn {
		panic("LLSN hole")
	}
	if i < len(s.committed) {
		// committed
		return fmt.Errorf("storage: could not delete committed (llsn=%v glsn=%v)", llsn, s.committed[i].glsn)
	}
	s.written = s.written[:i]
	return nil
}

func (s *InMemoryStorage) Close() error {
	return nil
}

func (s *InMemoryStorage) assert() {
	s.muCommitted.Lock()
	defer s.muCommitted.Unlock()

	s.muWritten.Lock()
	defer s.muWritten.Unlock()

	if len(s.written) < len(s.committed) {
		goto out
	}
	if len(s.committed) > 0 && s.written[0].llsn != s.committed[0].llsn {
		goto out
	}
	return
out:
	panic("bad storage")
}
