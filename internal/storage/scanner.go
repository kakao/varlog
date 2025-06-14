package storage

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/cockroachdb/pebble/v2"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

var scannerPool = sync.Pool{
	New: func() interface{} {
		s := &Scanner{}
		s.lazy.dkUpper = make([]byte, dataKeyLength)
		s.cks.lower = make([]byte, commitKeyLength)
		s.cks.upper = make([]byte, commitKeyLength)
		s.dks.lower = make([]byte, dataKeyLength)
		s.dks.upper = make([]byte, dataKeyLength)
		return s
	},
}

type Scanner struct {
	stg  *Storage
	it   *pebble.Iterator
	lazy struct {
		dataIt  *pebble.Iterator
		dkUpper []byte
	}
	cks struct {
		lower []byte
		upper []byte
	}
	dks struct {
		lower []byte
		upper []byte
	}

	scanConfig
}

func newScanner() *Scanner {
	return scannerPool.Get().(*Scanner)
}

func (s *Scanner) Valid() bool {
	return s.it.Valid()
}

func (s *Scanner) Value() (le varlogpb.LogEntry, err error) {
	if !s.Valid() {
		// TODO: replace io.EOF with ErrNoLogEntry.
		return le, io.EOF
	}
	if s.withGLSN {
		return s.valueByGLSN()
	}
	return s.valueByLLSN()
}

func (s *Scanner) Next() bool {
	if s.lazy.dataIt != nil {
		_ = s.lazy.dataIt.Next()
	}
	return s.it.Next()
}

func (s *Scanner) Close() (err error) {
	if s.it != nil {
		err = s.it.Close()
	}
	if s.lazy.dataIt != nil {
		if e := s.lazy.dataIt.Close(); e != nil {
			if err != nil {
				err = errors.Join(err, e)
			} else {
				err = e
			}
		}
	}
	s.release()
	return err
}

func (s *Scanner) valueByGLSN() (le varlogpb.LogEntry, err error) {
	ck := s.it.Key()
	dk := s.it.Value()
	llsn := decodeDataKey(dk)
	if s.lazy.dataIt == nil {
		err = s.initLazyIterator(dk, llsn)
		if err != nil {
			return le, err
		}
	}
	if slices.Compare(dk, s.lazy.dataIt.Key()) != 0 {
		return le, fmt.Errorf("%s: %w", s.stg.path, ErrInconsistentWriteCommitState)
	}

	le.GLSN = decodeCommitKey(ck)
	le.LLSN = llsn
	data := s.lazy.dataIt.Value()
	if len(data) > 0 {
		le.Data = make([]byte, len(data))
		copy(le.Data, data)
	}
	return le, nil
}

func (s *Scanner) initLazyIterator(beginKey []byte, beginLLSN types.LLSN) (err error) {
	endLLSN := beginLLSN + types.LLSN(s.end.GLSN-s.begin.GLSN)
	s.lazy.dkUpper = encodeDataKeyInternal(endLLSN, s.lazy.dkUpper)
	itOpt := &pebble.IterOptions{
		LowerBound: beginKey,
		UpperBound: s.lazy.dkUpper,
	}

	s.lazy.dataIt, err = s.stg.dataDB.NewIter(itOpt)
	if err != nil {
		return err
	}
	if !s.lazy.dataIt.First() {
		return fmt.Errorf("%s: %w", s.stg.path, ErrInconsistentWriteCommitState)
	}
	return nil
}

func (s *Scanner) valueByLLSN() (le varlogpb.LogEntry, err error) {
	le.LLSN = decodeDataKey(s.it.Key())
	if len(s.it.Value()) > 0 {
		le.Data = make([]byte, len(s.it.Value()))
		copy(le.Data, s.it.Value())
	}
	return le, nil
}

func (s *Scanner) release() {
	s.stg = nil
	s.it = nil
	s.lazy.dataIt = nil
	s.scanConfig = scanConfig{}
	scannerPool.Put(s)
}

//type ScanResult struct {
//	logEntry varlogpb.LogEntry
//	err      error
//}
//
//func (sr ScanResult) LogEntry() varlogpb.LogEntry {
//	return sr.logEntry
//}
//
//func (sr ScanResult) Valid() bool {
//	return sr.err == nil
//}
//
//func (sr ScanResult) Err() error {
//	return sr.err
//}

type scanConfig struct {
	withGLSN bool
	begin    varlogpb.LogEntryMeta
	end      varlogpb.LogEntryMeta
}

func newScanConfig(opts []ScanOption) scanConfig {
	cfg := scanConfig{}
	for _, opt := range opts {
		opt.applyScan(&cfg)
	}
	return cfg
}

type ScanOption interface {
	applyScan(*scanConfig)
}

type funcScanOption struct {
	f func(*scanConfig)
}

func newFuncScanOption(f func(*scanConfig)) *funcScanOption {
	return &funcScanOption{f: f}
}

func (fso *funcScanOption) applyScan(cfg *scanConfig) {
	fso.f(cfg)
}

func WithGLSN(begin, end types.GLSN) ScanOption {
	return newFuncScanOption(func(cfg *scanConfig) {
		cfg.withGLSN = true
		cfg.begin = varlogpb.LogEntryMeta{GLSN: begin}
		cfg.end = varlogpb.LogEntryMeta{GLSN: end}
	})
}

func WithLLSN(begin, end types.LLSN) ScanOption {
	return newFuncScanOption(func(cfg *scanConfig) {
		cfg.withGLSN = false
		cfg.begin = varlogpb.LogEntryMeta{LLSN: begin}
		cfg.end = varlogpb.LogEntryMeta{LLSN: end}
	})
}
