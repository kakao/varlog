package storage

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"go.uber.org/multierr"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

var (
	ErrNoLogEntry                   = errors.New("storage: no log entry")
	ErrNoCommitContext              = errors.New("storage: no commit context")
	ErrInconsistentWriteCommitState = errors.New("storage: inconsistent write and commit")
)

type Storage struct {
	config

	db        *pebble.DB
	writeOpts *pebble.WriteOptions
}

// New creates a new storage.
func New(opts ...Option) (*Storage, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	pebbleOpts := &pebble.Options{
		DisableWAL:                  !cfg.wal,
		L0CompactionThreshold:       cfg.l0CompactionThreshold,
		L0StopWritesThreshold:       cfg.l0StopWritesThreshold,
		LBaseMaxBytes:               cfg.lbaseMaxBytes,
		MaxOpenFiles:                cfg.maxOpenFiles,
		MemTableSize:                cfg.memTableSize,
		MemTableStopWritesThreshold: cfg.memTableStopWritesThreshold,
		MaxConcurrentCompactions:    cfg.maxConcurrentCompaction,
		Levels:                      make([]pebble.LevelOptions, 7),
		ErrorIfExists:               false,
	}
	for i := 0; i < len(pebbleOpts.Levels); i++ {
		l := &pebbleOpts.Levels[i]
		l.BlockSize = 32 << 10
		l.IndexBlockSize = 256 << 10
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = pebbleOpts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	pebbleOpts.Levels[6].FilterPolicy = nil
	pebbleOpts.FlushSplitBytes = pebbleOpts.Levels[0].TargetFileSize
	pebbleOpts.EnsureDefaults()

	if cfg.verbose {
		pebbleOpts.EventListener = pebble.MakeLoggingEventListener(newLogAdaptor(cfg.logger))
		pebbleOpts.EventListener.TableDeleted = nil
		pebbleOpts.EventListener.TableIngested = nil
		pebbleOpts.EventListener.WALCreated = nil
		pebbleOpts.EventListener.WALDeleted = nil
	}

	db, err := pebble.Open(cfg.path, pebbleOpts)
	if err != nil {
		return nil, err
	}
	return &Storage{
		config:    cfg,
		db:        db,
		writeOpts: &pebble.WriteOptions{Sync: false},
	}, nil
}

// NewWriteBatch creates a batch for write operations.
func (s *Storage) NewWriteBatch() *WriteBatch {
	return newWriteBatch(s.db.NewBatch(), s.writeOpts)
}

// NewCommitBatch creates a batch for commit operations.
func (s *Storage) NewCommitBatch(cc CommitContext) (*CommitBatch, error) {
	cb := newCommitBatch(s.db.NewBatch(), s.writeOpts)
	if err := cb.batch.Set(encodeCommitContextKeyInternal(cc, cb.cck), nil, nil); err != nil {
		_ = cb.Close()
		return nil, err
	}
	return cb, nil
}

// NewScanner creates a scanner for the given key range.
func (s *Storage) NewScanner(opts ...ScanOption) *Scanner {
	scanner := newScanner()
	scanner.scanConfig = newScanConfig(opts)
	scanner.stg = s
	itOpt := &pebble.IterOptions{}
	if scanner.withGLSN {
		itOpt.LowerBound = encodeCommitKeyInternal(scanner.begin.GLSN, scanner.cks.lower)
		itOpt.UpperBound = encodeCommitKeyInternal(scanner.end.GLSN, scanner.cks.upper)
	} else {
		itOpt.LowerBound = encodeDataKeyInternal(scanner.begin.LLSN, scanner.dks.lower)
		itOpt.UpperBound = encodeDataKeyInternal(scanner.end.LLSN, scanner.dks.upper)
	}
	scanner.it = s.db.NewIter(itOpt)
	_ = scanner.it.First()
	return scanner
}

// Read reads the log entry at the glsn.
func (s *Storage) Read(opts ...ReadOption) (le varlogpb.LogEntry, err error) {
	cfg := newReadConfig(opts)
	if !cfg.glsn.Invalid() {
		return s.readGLSN(cfg.glsn)
	}
	return s.readLLSN(cfg.llsn)
}

func (s *Storage) readGLSN(glsn types.GLSN) (le varlogpb.LogEntry, err error) {
	scanner := s.NewScanner(WithGLSN(glsn, glsn+1))
	defer func() {
		_ = scanner.Close()
	}()
	if !scanner.Valid() {
		return le, ErrNoLogEntry
	}
	return scanner.Value()
}

func (s *Storage) readLLSN(llsn types.LLSN) (le varlogpb.LogEntry, err error) {
	it := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitKeyPrefix},
		UpperBound: []byte{commitKeySentinelPrefix},
	})
	defer func() {
		_ = it.Close()
	}()

	ck := make([]byte, commitKeyLength)
	it.First()
	for it.Valid() {
		currLLSN := decodeDataKey(it.Value())
		if currLLSN > llsn {
			break
		}

		currGLSN := decodeCommitKey(it.Key())
		if currLLSN == llsn {
			return s.readGLSN(currGLSN)
		}

		delta := llsn - currLLSN
		glsnGuess := currGLSN + types.GLSN(delta)
		it.SeekGE(encodeCommitKeyInternal(glsnGuess, ck))
	}
	return le, ErrNoLogEntry
}

// CommitContextOf looks up a commit context that contains the log entry for the argument glsn.
func (s *Storage) CommitContextOf(glsn types.GLSN) (cc CommitContext, err error) {
	cck := make([]byte, commitContextKeyLength)
	cck = encodeCommitContextKeyInternal(CommitContext{HighWatermark: glsn}, cck)
	it := s.db.NewIter(&pebble.IterOptions{
		LowerBound: cck,
		UpperBound: []byte{commitContextKeySentinelPrefix},
	})
	defer func() {
		_ = it.Close()
	}()
	if it.First() {
		cc = decodeCommitContextKey(it.Key())
		if cc.CommittedGLSNBegin <= glsn && glsn < cc.CommittedGLSNEnd {
			return cc, nil
		}
	}
	return cc, ErrNoCommitContext
}

// NextCommitContextOf returns the next commit context after the argument cc if exists.
func (s *Storage) NextCommitContextOf(cc CommitContext) (next CommitContext, err error) {
	cck := make([]byte, commitContextKeyLength)
	cck = encodeCommitContextKeyInternal(cc, cck)
	it := s.db.NewIter(&pebble.IterOptions{
		LowerBound: cck,
		UpperBound: []byte{commitContextKeySentinelPrefix},
	})
	defer func() {
		_ = it.Close()
	}()
	if !it.First() || !decodeCommitContextKey(it.Key()).Equal(cc) || !it.Next() {
		return next, ErrNoCommitContext
	}
	return decodeCommitContextKey(it.Key()), nil
}

// Trim deletes log entries whose GLSNs are less than or equal to the argument glsn.
// It returns the ErrNoCommitContext if there are no logs to delete.
func (s *Storage) Trim(glsn types.GLSN) error {
	lem, err := s.findLTE(glsn)
	if err != nil {
		return err
	}

	trimGLSN, trimLLSN := lem.GLSN, lem.LLSN

	batch := s.db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()

	// commit context
	cc, err := s.CommitContextOf(trimGLSN)
	if err != nil {
		return err
	}
	cckBegin := make([]byte, commitContextKeyLength)
	cckBegin = encodeCommitContextKeyInternal(CommitContext{}, cckBegin)
	cckEnd := make([]byte, commitContextKeyLength)
	cckEnd = encodeCommitContextKeyInternal(cc, cckEnd)
	if cc.CommittedGLSNEnd-1 == trimGLSN {
		// cc is deletable.
		cc.Version++
		cckEnd = encodeCommitContextKeyInternal(cc, cckEnd)
	}
	if err := batch.DeleteRange(cckBegin, cckEnd, nil); err != nil {
		return err
	}

	// commit
	ckBegin := make([]byte, commitKeyLength)
	ckBegin = encodeCommitKeyInternal(types.MinGLSN, ckBegin)
	ckEnd := make([]byte, commitKeyLength)
	ckEnd = encodeCommitKeyInternal(trimGLSN+1, ckEnd)
	if err := batch.DeleteRange(ckBegin, ckEnd, nil); err != nil {
		return err
	}

	// data
	dkBegin := make([]byte, dataKeyLength)
	dkBegin = encodeDataKeyInternal(types.MinLLSN, dkBegin)
	dkEnd := make([]byte, dataKeyLength)
	dkEnd = encodeDataKeyInternal(trimLLSN+1, dkEnd)
	if err := batch.DeleteRange(dkBegin, dkEnd, nil); err != nil {
		return err
	}

	return batch.Commit(s.writeOpts)
}

func (s *Storage) findLTE(glsn types.GLSN) (lem varlogpb.LogEntryMeta, err error) {
	ck := make([]byte, commitKeyLength)
	it := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitKeyPrefix},
		UpperBound: encodeCommitKeyInternal(glsn+1, ck),
	})
	defer func() {
		_ = it.Close()
	}()
	if !it.Last() {
		return lem, ErrNoLogEntry
	}
	lem.GLSN = decodeCommitKey(it.Key())
	lem.LLSN = decodeDataKey(it.Value())
	return lem, nil
}

// Path returns the path to the storage.
func (s Storage) Path() string {
	return s.path
}

// Close closes the storage.
func (s *Storage) Close() error {
	return multierr.Append(s.db.Flush(), s.db.Close())
}
