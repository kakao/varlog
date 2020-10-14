package storage

import (
	"errors"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

var errEndOfRange = errors.New("storage: end of range")

// ScanResult represents a result of Scanner.Next() method. It should be immutable.
type ScanResult struct {
	LogEntry varlog.LogEntry
	Err      error
}

func newInvalidScanResult(err error) ScanResult {
	return ScanResult{
		LogEntry: varlog.InvalidLogEntry,
		Err:      err,
	}
}

func (sr ScanResult) Valid() bool {
	return sr.Err == nil
}

// Scanner scans the log entries which are range speicified by Storage.Scan() method.
type Scanner interface {
	// Next returns log entries sequentially. If something wrong happens, it returns an error.
	Next() ScanResult

	// Close releases resources acquired by scanner.
	Close() error
}

type Storage interface {
	// Read reads the log entry at the glsn.
	// If there is no entry at the given position, it returns varlog.ErrNoEntry.
	Read(glsn types.GLSN) (varlog.LogEntry, error)

	// Scan returns Scanner that reads log entries from the glsn.
	Scan(begin, end types.GLSN) (Scanner, error)

	// Write writes log entry at the llsn. The log entry contains data.
	Write(llsn types.LLSN, data []byte) error

	// Commit confirms that the log entry at the llsn is assigned global log position with the
	// glsn.
	Commit(llsn types.LLSN, glsn types.GLSN) error

	// DeleteCommitted removes committed log entries until the glsn. It acts like garbage collection.
	DeleteCommitted(glsn types.GLSN) error

	// DeleteUncommitted removes uncommitted log entries from the llsn. It should not remove
	// committed log entries. If the log entry at the given llsn is committed, it panics.
	DeleteUncommitted(llsn types.LLSN) error

	// Close closes the storage.
	Close() error
}
