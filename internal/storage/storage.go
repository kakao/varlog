package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"

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

	valueStore  *store
	commitStore *store

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

	// Check directory entries in s.path to see whether anything except
	// valueStoreDirName and commitStoreDirName exists, which results in an
	// error if it exists.
	ds, err := os.ReadDir(s.path)
	if err != nil {
		return nil, err
	}
	for _, d := range ds {
		if name := d.Name(); name != valueStoreDirName && name != commitStoreDirName {
			return nil, fmt.Errorf("forbidden entry: %s", name)
		}
	}

	s.valueStore, err = newStore(
		filepath.Join(s.path, valueStoreDirName),
		slices.Concat(s.valueStoreOptions, []StoreOption{
			withLogger(s.logger.With(zap.String("store", "value"))),
		})...,
	)
	if err != nil {
		return nil, err
	}

	s.commitStore, err = newStore(
		filepath.Join(s.path, commitStoreDirName),
		slices.Concat(s.commitStoreOptions, []StoreOption{
			withLogger(s.logger.With(zap.String("store", "commit"))),
		})...,
	)
	if err != nil {
		return nil, err
	}

	s.startMetricsLogger()

	return s, nil
}

// NewWriteBatch creates a batch for write operations.
func (s *Storage) NewWriteBatch() *WriteBatch {
	return newWriteBatch(s.valueStore)
}

// NewCommitBatch creates a batch for commit operations.
func (s *Storage) NewCommitBatch(cc CommitContext) (*CommitBatch, error) {
	cb := newCommitBatch(s.commitStore)
	if err := cb.batch.Set(commitContextKey, encodeCommitContext(cc, cb.cc), nil); err != nil {
		_ = cb.Close()
		return nil, err
	}
	return cb, nil
}

// NewAppendBatch creates a batch for appending log entries. It does not put
// commit context.
func (s *Storage) NewAppendBatch() *AppendBatch {
	return newAppendBatch(s.valueStore.db.NewBatch(), s.commitStore.db.NewBatch(), s.valueStore.writeOpts)
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
		scanner.it, err = s.commitStore.db.NewIter(itOpt)
	} else {
		itOpt.LowerBound = encodeDataKeyInternal(scanner.begin.LLSN, scanner.dks.lower)
		itOpt.UpperBound = encodeDataKeyInternal(scanner.end.LLSN, scanner.dks.upper)
		scanner.it, err = s.valueStore.db.NewIter(itOpt)
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
	it, err := s.commitStore.db.NewIter(&pebble.IterOptions{
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
	buf, closer, err := s.commitStore.db.Get(commitContextKey)
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

	dataBatch := s.valueStore.db.NewBatch()
	commitBatch := s.commitStore.db.NewBatch()
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

	return errors.Join(commitBatch.Commit(s.commitStore.writeOpts), dataBatch.Commit(s.valueStore.writeOpts))
}

func (s *Storage) findLTE(glsn types.GLSN) (lem varlogpb.LogEntryMeta, err error) {
	var upper []byte
	if glsn < types.MaxGLSN {
		upper = make([]byte, commitKeyLength)
		upper = encodeCommitKeyInternal(glsn+1, upper)
	} else {
		upper = []byte{commitKeySentinelPrefix}
	}

	it, err := s.commitStore.db.NewIter(&pebble.IterOptions{
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
	usage := s.valueStore.db.Metrics().DiskSpaceUsage()
	usage += s.commitStore.db.Metrics().DiskSpaceUsage()
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
				fmt.Fprintf(&sb, "ValueStore Metrics\n%sCommitStore Metrics\n%s", s.valueStore.db.Metrics(), s.commitStore.db.Metrics())
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
	s.stopMetricsLogger()

	err = s.valueStore.close()
	return errors.Join(err, s.commitStore.close())
}
