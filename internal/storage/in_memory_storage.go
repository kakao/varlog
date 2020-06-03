package storage

import (
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

type memLogEntryStatus int

const (
	Unwritten memLogEntryStatus = iota
	Written
	Junk
	Trimmed
)

type inMemoryLogEntry struct {
	status memLogEntryStatus
	data   []byte
}

type InMemoryStorage struct {
	epoch          uint64
	lastTrimmedLsn uint64
	maxLsn         uint64
	logStream      []inMemoryLogEntry
	mu             sync.RWMutex
}

func NewInMemoryStorage(initalLogStreamSize int) *InMemoryStorage {
	return &InMemoryStorage{
		epoch:          0,
		lastTrimmedLsn: 0,
		maxLsn:         0,
		logStream:      make([]inMemoryLogEntry, initalLogStreamSize),
	}
}

func (s *InMemoryStorage) Read(epoch uint64, glsn uint64) ([]byte, error) {
	if epoch != s.epoch {
		return nil, varlog.ErrSealedEpoch
	}
	return s.read(glsn)
}

func (s *InMemoryStorage) Append(epoch uint64, glsn uint64, data []byte) error {
	if epoch != s.epoch {
		return varlog.ErrSealedEpoch
	}
	logEntry := inMemoryLogEntry{
		status: Written,
		data:   data,
	}
	return s.write(glsn, logEntry)
}

func (s *InMemoryStorage) Fill(epoch uint64, glsn uint64) error {
	if epoch != s.epoch {
		return varlog.ErrSealedEpoch
	}
	logEntry := inMemoryLogEntry{
		status: Junk,
	}
	return s.write(glsn, logEntry)
}

func (s *InMemoryStorage) Trim(epoch uint64, glsn uint64) error {
	if epoch != s.epoch {
		return varlog.ErrSealedEpoch
	}
	if !s.hasRoom(glsn) {
		return varlog.ErrUnwrittenLogEntry
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := s.lastTrimmedLsn + 1; i <= glsn; i++ {
		s.logStream[i].status = Trimmed
	}
	s.lastTrimmedLsn = glsn
	return nil
}

func (s *InMemoryStorage) Seal(epoch uint64, maxLsn *uint64) error {
	s.mu.Lock()
	if epoch > s.epoch {
		s.epoch = epoch
	}
	s.mu.Unlock()
	s.GetHighLSN(maxLsn)
	return nil
}

func (s *InMemoryStorage) GetHighLSN(lsn *uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	*lsn = s.maxLsn
	return nil
}

func (s *InMemoryStorage) GetEpoch() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epoch
}

func (s *InMemoryStorage) size() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return uint64(len(s.logStream))
}

func (s *InMemoryStorage) hasRoom(glsn uint64) bool {
	return s.size() >= glsn+1
}

func (s *InMemoryStorage) expand(glsn uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldLen := uint64(len(s.logStream))
	if oldLen < glsn+1 {
		s.logStream = append(s.logStream, make([]inMemoryLogEntry, oldLen*2)...)
	}
}

func (s *InMemoryStorage) read(glsn uint64) ([]byte, error) {
	if !s.hasRoom(glsn) {
		return nil, varlog.ErrUnwrittenLogEntry
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	logEntry := s.logStream[glsn]
	switch logEntry.status {
	case Unwritten:
		return nil, varlog.ErrUnwrittenLogEntry
	case Trimmed:
		return nil, varlog.ErrTrimmedLogEntry
	}
	return logEntry.data, nil
}

func (s *InMemoryStorage) write(glsn uint64, logEntry inMemoryLogEntry) error {
	if !s.hasRoom(glsn) {
		s.expand(glsn)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	logEntryPosition := &s.logStream[glsn]
	switch logEntryPosition.status {
	case Written, Junk:
		return varlog.ErrWrittenLogEntry
	case Trimmed:
		return varlog.ErrTrimmedLogEntry
	}
	logEntryPosition.status = logEntry.status
	logEntryPosition.data = logEntry.data
	if glsn > s.maxLsn {
		s.maxLsn = glsn
	}
	return nil
}
