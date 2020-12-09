package storagenode

import (
	"fmt"
	"sort"
	"sync"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

const (
	InMemoryStorageName      = "inmem"
	defaultInMemoryBatchSize = 32
)

type writtenEntry struct {
	llsn types.LLSN
	data []byte
}

type committedEntry struct {
	llsn types.LLSN
	glsn types.GLSN
}

type inMemoryWriteBatch struct {
	entries []WriteEntry
	s       *InMemoryStorage
}

func (iwb *inMemoryWriteBatch) Put(llsn types.LLSN, data []byte) error {
	iwb.entries = append(iwb.entries, WriteEntry{LLSN: llsn, Data: data})
	return nil
}

func (iwb *inMemoryWriteBatch) Apply() error {
	return iwb.s.WriteBatch(iwb.entries)
}

func (iwb *inMemoryWriteBatch) Close() error {
	for i := 0; i < len(iwb.entries); i++ {
		iwb.entries[i].Data = nil
	}
	iwb.entries = nil
	return nil
}

type inMemoryCommitBatch struct {
	entries []CommitEntry
	s       *InMemoryStorage
}

func (icb *inMemoryCommitBatch) Put(llsn types.LLSN, glsn types.GLSN) error {
	icb.entries = append(icb.entries, CommitEntry{LLSN: llsn, GLSN: glsn})
	return nil
}

func (icb *inMemoryCommitBatch) Apply() error {
	return icb.s.CommitBatch(icb.entries)
}

func (icb *inMemoryCommitBatch) Close() error {
	icb.entries = nil
	return nil
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
		return NewInvalidScanResult(ErrEndOfRange)
	}
	went := s.writtenSnapshot[s.cursor]
	cent := s.committedSnapshot[s.cursor]
	if went.llsn != cent.llsn {
		// TODO (jun): storage is broken
		s.storage.logger.Panic("inconsistent storage: written != committed")
	}

	result := ScanResult{
		LogEntry: types.LogEntry{
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

func newInMemoryStorage(opts *StorageOptions) (Storage, error) {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	opts.Logger = opts.Logger.Named("memstorage")
	return &InMemoryStorage{logger: opts.Logger}, nil
}

func (s *InMemoryStorage) RecoverLogStreamContext(lsc *logStreamContext) bool {
	return false
	// panic("not implemented")
}

// TODO (jun): consider in-memory storage
func (s *InMemoryStorage) Path() string {
	return ":memory"
}

func (s *InMemoryStorage) Name() string {
	return InMemoryStorageName
}

func (s *InMemoryStorage) searchCommittedEntry(glsn types.GLSN) (int, committedEntry, error) {
	i := sort.Search(len(s.committed), func(i int) bool { return s.committed[i].glsn >= glsn })
	if i >= len(s.committed) {
		return i, committedEntry{}, verrors.ErrNoEntry
	}
	if s.committed[i].glsn == glsn {
		return i, s.committed[i], nil
	}
	return i, s.committed[i], verrors.ErrNoEntry
}

func (s *InMemoryStorage) Read(glsn types.GLSN) (types.LogEntry, error) {
	s.assert()
	defer s.assert()

	s.muCommitted.RLock()
	defer s.muCommitted.RUnlock()
	if len(s.committed) == 0 {
		return types.InvalidLogEntry, verrors.ErrNoEntry
	}

	first := s.committed[0]
	last := s.committed[len(s.committed)-1]
	if first.glsn > glsn || last.glsn < glsn {
		return types.InvalidLogEntry, verrors.ErrNoEntry
	}

	i, _, err := s.searchCommittedEntry(glsn)
	if err != nil {
		return types.InvalidLogEntry, verrors.ErrNoEntry
	}
	// NB: The LLSN of the first entry of written and committed should be same.
	// NB: committedEntry[i] and writtenEntry[i] are the same log entry.
	s.muWritten.RLock()
	defer s.muWritten.RUnlock()
	went := s.written[i]
	return types.LogEntry{
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
		return nil, verrors.ErrInvalid
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
		s.logger.Panic("try to write incorrect LLSN", zap.Any("prev_llsn", s.written[len(s.written)-1].llsn), zap.Any("curr_llsn", llsn))
	}

	s.written = append(s.written, writtenEntry{llsn: llsn, data: data})
	return nil
}

func (s *InMemoryStorage) NewWriteBatch() WriteBatch {
	return &inMemoryWriteBatch{
		entries: make([]WriteEntry, 0, defaultInMemoryBatchSize),
		s:       s,
	}
}

// FIXME (jun): WriteBatch should be atomic.
func (s *InMemoryStorage) WriteBatch(entries []WriteEntry) error {
	for _, entry := range entries {
		s.logger.Debug("write_batch", zap.Any("llsn", entry.LLSN))
		if err := s.Write(entry.LLSN, entry.Data); err != nil {
			return err
		}
	}
	return nil
}

func (s *InMemoryStorage) searchWrittenEnry(llsn types.LLSN) (int, writtenEntry, error) {
	i := sort.Search(len(s.written), func(i int) bool { return s.written[i].llsn >= llsn })
	if i >= len(s.written) {
		return i, writtenEntry{}, verrors.ErrNoEntry
	}
	if s.written[i].llsn == llsn {
		return i, s.written[i], nil
	}
	return i, s.written[i], verrors.ErrNoEntry
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
			return verrors.ErrInvalid
		}
	}
	s.committed = append(s.committed, committedEntry{llsn: llsn, glsn: glsn})
	return nil
}

func (s *InMemoryStorage) NewCommitBatch() CommitBatch {
	return &inMemoryCommitBatch{
		entries: make([]CommitEntry, 0, defaultInMemoryBatchSize),
		s:       s,
	}
}

func (s *InMemoryStorage) CommitBatch(entries []CommitEntry) error {
	for _, entry := range entries {
		if err := s.Commit(entry.LLSN, entry.GLSN); err != nil {
			return err
		}
	}
	return nil
}

func (s *InMemoryStorage) StoreCommitContext(hwm, prevHWM, committedGLSNBegin, committedGLSNEnd types.GLSN) error {
	return nil
	// panic("not implemented")
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
