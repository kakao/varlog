package storage

import (
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

var scannerPool = sync.Pool{
	New: func() interface{} {
		s := &Scanner{}
		s.cks.lower = make([]byte, commitKeyLength)
		s.cks.upper = make([]byte, commitKeyLength)
		s.dks.lower = make([]byte, dataKeyLength)
		s.dks.upper = make([]byte, dataKeyLength)
		return s
	},
}

type Scanner struct {
	scanConfig
	stg *Storage
	it  *pebble.Iterator
	cks struct {
		lower []byte
		upper []byte
	}
	dks struct {
		lower []byte
		upper []byte
	}
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
	return s.it.Next()
}

func (s *Scanner) Close() error {
	err := s.it.Close()
	s.release()
	return err
}

func (s *Scanner) valueByGLSN() (le varlogpb.LogEntry, err error) {
	ck := s.it.Key()
	dk := s.it.Value()
	data, closer, err := s.stg.db.Get(dk)
	if err != nil {
		if err == pebble.ErrNotFound {
			return le, fmt.Errorf("%s: %w", s.stg.path, ErrInconsistentWriteCommitState)
		}
		return le, err
	}
	le.GLSN = decodeCommitKey(ck)
	le.LLSN = decodeDataKey(dk)
	if len(data) > 0 {
		le.Data = make([]byte, len(data))
		copy(le.Data, data)
	}
	_ = closer.Close()
	return le, nil
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
	s.scanConfig = scanConfig{}
	s.stg = nil
	s.it = nil
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
