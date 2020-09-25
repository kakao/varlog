package storage

import (
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
)

type Scanner interface {
	Next() (varlog.LogEntry, error)
}

type Storage interface {
	// Read reads the log entry at the glsn.
	// If there is no entry at the given position, it returns varlog.ErrNoEntry.
	Read(glsn types.GLSN) (varlog.LogEntry, error)

	ReadByLLSN(llsn types.LLSN) (varlog.LogEntry, error)

	// Scan returns Scanner that reads log entries from the glsn.
	Scan(glsn types.GLSN) (Scanner, error)

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
