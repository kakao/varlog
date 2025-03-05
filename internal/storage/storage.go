package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"

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

	dataDB          *pebble.DB
	dataDBWriteOpts *pebble.WriteOptions

	commitDB          *pebble.DB
	commitDBWriteOpts *pebble.WriteOptions

	metricsLogger struct {
		wg     sync.WaitGroup
		ticker *time.Ticker
		stop   chan struct{}
	}
}

// New creates a new storage.
func New(opts ...Option) (*Storage, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		config: cfg,
	}

	if cfg.separateDB {
		// Check directory entries in s.path to see whether anything except
		// dataDBDirName and commitDBDirName exists, which results in an error
		// if it exists.
		ds, err := os.ReadDir(s.path)
		if err != nil {
			return nil, err
		}
		for _, d := range ds {
			if name := d.Name(); name != dataDBDirName && name != commitDBDirName {
				return nil, fmt.Errorf("forbidden entry: %s", name)
			}
		}
		dataDBConfig, err := newDBConfig(s.dataDBOptions...)
		if err != nil {
			return nil, err
		}
		s.dataDBWriteOpts = &pebble.WriteOptions{Sync: dataDBConfig.sync}
		s.dataDB, err = s.newDB(filepath.Join(s.path, dataDBDirName), &dataDBConfig, s.cache)
		if err != nil {
			return nil, err
		}
		commitDBConfig, err := newDBConfig(s.commitDBOptions...)
		if err != nil {
			return nil, err
		}
		s.commitDBWriteOpts = &pebble.WriteOptions{Sync: commitDBConfig.sync}
		s.commitDB, err = s.newDB(filepath.Join(s.path, commitDBDirName), &commitDBConfig, s.cache)
		if err != nil {
			return nil, err
		}
	} else {
		// Check directory entries in s.path to see whether dataDBDirName and
		// commitDBDirName exist, which results in an error if it exists.
		for _, dbDirName := range []string{dataDBDirName, commitDBDirName} {
			if _, err := os.Stat(filepath.Join(s.path, dbDirName)); err == nil {
				return nil, fmt.Errorf("non-separating database, but %s exists", dbDirName)
			}
		}
		dataDBConfig, err := newDBConfig(s.dataDBOptions...)
		if err != nil {
			return nil, err
		}
		s.dataDBWriteOpts = &pebble.WriteOptions{Sync: dataDBConfig.sync}
		s.dataDB, err = s.newDB(s.path, &dataDBConfig, s.cache)
		if err != nil {
			return nil, err
		}

		s.commitDB = s.dataDB
		s.commitDBWriteOpts = s.dataDBWriteOpts
	}
	s.startMetricsLogger()

	return s, nil
}

func (s *Storage) newDB(path string, cfg *dbConfig, cache *Cache) (*pebble.DB, error) {
	pebbleOpts := &pebble.Options{
		Cache:                       cache.get(),
		DisableWAL:                  !cfg.wal,
		L0CompactionFileThreshold:   cfg.l0CompactionFileThreshold,
		L0CompactionThreshold:       cfg.l0CompactionThreshold,
		L0StopWritesThreshold:       cfg.l0StopWritesThreshold,
		LBaseMaxBytes:               cfg.lbaseMaxBytes,
		MaxOpenFiles:                cfg.maxOpenFiles,
		MemTableSize:                uint64(cfg.memTableSize),
		MemTableStopWritesThreshold: cfg.memTableStopWritesThreshold,
		MaxConcurrentCompactions:    func() int { return cfg.maxConcurrentCompaction },
		Levels:                      make([]pebble.LevelOptions, 7),
		ErrorIfExists:               false,
		FlushDelayDeleteRange:       s.trimDelay,
		TargetByteDeletionRate:      s.trimRateByte,
	}
	pebbleOpts.Levels[0].TargetFileSize = cfg.l0TargetFileSize
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
	pebbleOpts.FlushSplitBytes = cfg.flushSplitBytes
	pebbleOpts.EnsureDefaults()

	if s.verbose {
		el := pebble.MakeLoggingEventListener(newLogAdaptor(s.logger))
		pebbleOpts.EventListener = &el
		// BackgroundError, DiskSlow, WriteStallBegin, WriteStallEnd
		pebbleOpts.EventListener.CompactionBegin = nil
		pebbleOpts.EventListener.CompactionEnd = nil
		pebbleOpts.EventListener.FlushBegin = nil
		pebbleOpts.EventListener.FlushEnd = nil
		pebbleOpts.EventListener.FormatUpgrade = nil
		pebbleOpts.EventListener.ManifestCreated = nil
		pebbleOpts.EventListener.ManifestDeleted = nil
		pebbleOpts.EventListener.TableCreated = nil
		pebbleOpts.EventListener.TableDeleted = nil
		pebbleOpts.EventListener.TableIngested = nil
		pebbleOpts.EventListener.TableStatsLoaded = nil
		pebbleOpts.EventListener.TableValidated = nil
		pebbleOpts.EventListener.WALCreated = nil
		pebbleOpts.EventListener.WALDeleted = nil
	}
	if s.readOnly {
		pebbleOpts.ReadOnly = true
	}
	var sb strings.Builder
	sb.WriteString("opening database: path=")
	sb.WriteString(path)
	if s.verbose {
		sb.WriteString("\n")
		sb.WriteString(pebbleOpts.String())
	}
	s.logger.Info(sb.String())
	return pebble.Open(path, pebbleOpts)
}

// NewWriteBatch creates a batch for write operations.
func (s *Storage) NewWriteBatch() *WriteBatch {
	return newWriteBatch(s.dataDB.NewBatch(), s.dataDBWriteOpts)
}

// NewCommitBatch creates a batch for commit operations.
func (s *Storage) NewCommitBatch(cc CommitContext) (*CommitBatch, error) {
	cb := newCommitBatch(s.commitDB.NewBatch(), s.commitDBWriteOpts)
	if err := cb.batch.Set(commitContextKey, encodeCommitContext(cc, cb.cc), nil); err != nil {
		_ = cb.Close()
		return nil, err
	}
	return cb, nil
}

// NewAppendBatch creates a batch for appending log entries. It does not put
// commit context.
func (s *Storage) NewAppendBatch() *AppendBatch {
	return newAppendBatch(s.dataDB.NewBatch(), s.commitDB.NewBatch(), s.dataDBWriteOpts)
}

// NewScanner creates a scanner for the given key range.
func (s *Storage) NewScanner(opts ...ScanOption) (scanner *Scanner, err error) {
	scanner = newScanner()
	scanner.scanConfig = newScanConfig(opts)
	scanner.stg = s
	itOpt := &pebble.IterOptions{}
	if scanner.withGLSN {
		itOpt.LowerBound = encodeCommitKeyInternal(scanner.begin.GLSN, scanner.cks.lower)
		itOpt.UpperBound = encodeCommitKeyInternal(scanner.end.GLSN, scanner.cks.upper)
		scanner.it, err = s.commitDB.NewIter(itOpt)
	} else {
		itOpt.LowerBound = encodeDataKeyInternal(scanner.begin.LLSN, scanner.dks.lower)
		itOpt.UpperBound = encodeDataKeyInternal(scanner.end.LLSN, scanner.dks.upper)
		scanner.it, err = s.dataDB.NewIter(itOpt)
	}
	if err != nil {
		_ = scanner.Close()
		return nil, err
	}
	_ = scanner.it.First()
	return scanner, nil
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
	scanner, err := s.NewScanner(WithGLSN(glsn, glsn+1))
	if err != nil {
		return
	}
	defer func() {
		_ = scanner.Close()
	}()
	if !scanner.Valid() {
		return le, ErrNoLogEntry
	}
	return scanner.Value()
}

func (s *Storage) readLLSN(llsn types.LLSN) (le varlogpb.LogEntry, err error) {
	it, err := s.commitDB.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitKeyPrefix},
		UpperBound: []byte{commitKeySentinelPrefix},
	})
	if err != nil {
		return
	}
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

func (s *Storage) ReadCommitContext() (cc CommitContext, err error) {
	buf, closer, err := s.commitDB.Get(commitContextKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			err = ErrNoCommitContext
		}
		return
	}
	defer func() {
		_ = closer.Close()
	}()
	return decodeCommitContext(buf), nil
}

// Trim deletes log entries whose GLSNs are less than or equal to the argument
// glsn. Internally, it removes records for both data and commits but does not
// remove the commit context.
// It returns the ErrNoLogEntry if there are no logs to delete.
func (s *Storage) Trim(glsn types.GLSN) error {
	lem, err := s.findLTE(glsn)
	if err != nil {
		return err
	}

	trimGLSN, trimLLSN := lem.GLSN, lem.LLSN

	dataBatch := s.dataDB.NewBatch()
	commitBatch := s.commitDB.NewBatch()
	defer func() {
		_ = dataBatch.Close()
		_ = commitBatch.Close()
	}()

	// commit
	ckBegin := make([]byte, commitKeyLength)
	ckBegin = encodeCommitKeyInternal(types.MinGLSN, ckBegin)
	ckEnd := make([]byte, commitKeyLength)
	ckEnd = encodeCommitKeyInternal(trimGLSN+1, ckEnd)
	_ = commitBatch.DeleteRange(ckBegin, ckEnd, nil)

	// data
	dkBegin := make([]byte, dataKeyLength)
	dkBegin = encodeDataKeyInternal(types.MinLLSN, dkBegin)
	dkEnd := make([]byte, dataKeyLength)
	dkEnd = encodeDataKeyInternal(trimLLSN+1, dkEnd)
	_ = dataBatch.DeleteRange(dkBegin, dkEnd, nil)

	return errors.Join(commitBatch.Commit(s.commitDBWriteOpts), dataBatch.Commit(s.dataDBWriteOpts))
}

func (s *Storage) findLTE(glsn types.GLSN) (lem varlogpb.LogEntryMeta, err error) {
	var upper []byte
	if glsn < types.MaxGLSN {
		upper = make([]byte, commitKeyLength)
		upper = encodeCommitKeyInternal(glsn+1, upper)
	} else {
		upper = []byte{commitKeySentinelPrefix}
	}

	it, err := s.commitDB.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitKeyPrefix},
		UpperBound: upper,
	})
	if err != nil {
		return
	}
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
func (s *Storage) Path() string {
	return s.path
}

func (s *Storage) DiskUsage() uint64 {
	usage := s.dataDB.Metrics().DiskSpaceUsage()
	if s.separateDB {
		usage += s.commitDB.Metrics().DiskSpaceUsage()
	}
	return usage
}

func (s *Storage) startMetricsLogger() {
	if s.metricsLogInterval <= 0 {
		return
	}
	s.metricsLogger.stop = make(chan struct{})
	s.metricsLogger.ticker = time.NewTicker(s.metricsLogInterval)
	s.metricsLogger.wg.Add(1)
	go func() {
		defer s.metricsLogger.wg.Done()
		for {
			select {
			case <-s.metricsLogger.ticker.C:
				var sb strings.Builder
				if s.separateDB {
					fmt.Fprintf(&sb, "DataDB Metrics\n%sCommitDB Metrics\n%s", s.dataDB.Metrics(), s.commitDB.Metrics())
				} else {
					fmt.Fprintf(&sb, "DB Metrics\n%s", s.dataDB.Metrics())
				}
				s.logger.Info(sb.String())
			case <-s.metricsLogger.stop:
				return
			}
		}
	}()
}

func (s *Storage) stopMetricsLogger() {
	if s.metricsLogInterval <= 0 {
		return
	}
	s.metricsLogger.ticker.Stop()
	close(s.metricsLogger.stop)
	s.metricsLogger.wg.Wait()
}

// Close closes the storage.
func (s *Storage) Close() (err error) {
	if !s.readOnly {
		err = s.dataDB.Flush()
		if s.separateDB {
			err = errors.Join(err, s.commitDB.Flush())
		}
	}
	s.stopMetricsLogger()

	err = errors.Join(err, s.dataDB.Close())
	if s.separateDB {
		err = errors.Join(err, s.commitDB.Close())
	}
	return err
}
