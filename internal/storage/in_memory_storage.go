package storage

import (
	"fmt"
	"sort"
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"go.uber.org/zap"
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

	logger *zap.Logger
}

type InMemoryScanner struct {
	begin             types.GLSN
	end               types.GLSN
	cursor            int
	writtenSnapshot   []writtenEntry
	committedSnapshot []committedEntry
	storage           *InMemoryStorage
}

func (s *InMemoryScanner) Next() ScanResult {
	if s.cursor >= len(s.committedSnapshot) {
		return newInvalidScanResult(errEndOfRange)
	}
	went := s.writtenSnapshot[s.cursor]
	cent := s.committedSnapshot[s.cursor]
	if went.llsn != cent.llsn {
		// TODO (jun): storage is broken
		s.storage.logger.Panic("inconsistent storage: written != committed")
	}

	result := ScanResult{
		LogEntry: varlog.LogEntry{
			LLSN: s.committedSnapshot[s.cursor].llsn,
			GLSN: s.committedSnapshot[s.cursor].glsn,
			// TODO(jun): copy byte array
			Data: s.writtenSnapshot[s.cursor].data,
		},
	}
	s.cursor++
	return result
}

func (s *InMemoryScanner) Close() error {
	return nil
}

func NewInMemoryStorage(logger *zap.Logger) Storage {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("memstorage")
	return &InMemoryStorage{logger: logger}
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

func (s *InMemoryStorage) Scan(begin, end types.GLSN) (Scanner, error) {
	s.assert()
	defer s.assert()

	// TODO (jun): consider reverse-scan
	if begin >= end {
		return nil, varlog.ErrInvalid
	}

	s.muCommitted.RLock()
	defer s.muCommitted.RUnlock()

	i, _, _ := s.searchCommittedEntry(begin)
	j, _, _ := s.searchCommittedEntry(end)
	committedSnapshot := s.committed[i:j]
	writtenSnapshot := s.written[i:j]

	scanner := &InMemoryScanner{
		begin:             begin,
		end:               end,
		committedSnapshot: committedSnapshot,
		writtenSnapshot:   writtenSnapshot,
		storage:           s,
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

func (s *InMemoryStorage) searchWrittenEnry(llsn types.LLSN) (int, writtenEntry, error) {
	i := sort.Search(len(s.written), func(i int) bool { return s.written[i].llsn >= llsn })
	if i >= len(s.written) {
		return i, writtenEntry{}, varlog.ErrNoEntry
	}
	if s.written[i].llsn == llsn {
		return i, s.written[i], nil
	}
	return i, s.written[i], varlog.ErrNoEntry
}

func (s *InMemoryStorage) Commit(llsn types.LLSN, glsn types.GLSN) error {
	s.assert()
	defer s.assert()

	s.muWritten.RLock()
	_, _, err := s.searchWrittenEnry(llsn)
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
	s.assert()
	defer s.assert()

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
