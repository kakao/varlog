package storagenode

import (
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

var ErrEndOfRange = errors.New("storage: end of range")

// ScanResult represents a result of Scanner.Next() method. It should be immutable.
type ScanResult struct {
	LogEntry types.LogEntry
	Err      error
}

func NewInvalidScanResult(err error) ScanResult {
	return ScanResult{
		LogEntry: types.InvalidLogEntry,
		Err:      err,
	}
}

func (sr ScanResult) Valid() bool {
	return sr.Err == nil
}

// Scanner scans the log entries which are range specified by Storage.Scan() method.
type Scanner interface {
	// Next returns log entries sequentially. If something wrong happens, it returns an error.
	Next() ScanResult

	// Close releases resources acquired by scanner.
	Close() error
}

type WriteEntry struct {
	LLSN types.LLSN
	Data []byte
}

type WriteBatch interface {
	Put(llsn types.LLSN, data []byte) error
	Apply() error
	Close() error
}

type CommitEntry struct {
	LLSN types.LLSN
	GLSN types.GLSN
}

type CommitBatch interface {
	Put(llsn types.LLSN, glsn types.GLSN) error
	Apply() error
	Close() error
}

type Storage interface {
	// Name returns the storage unique name.
	Name() string

	// Path returns directory to store storage data files.
	Path() string

	// Read reads the log entry at the glsn.
	// If there is no entry at the given position, it returns varlog.ErrNoEntry.
	Read(glsn types.GLSN) (types.LogEntry, error)

	// Scan returns Scanner that reads log entries from the glsn.
	Scan(begin, end types.GLSN) (Scanner, error)

	// Write writes log entry at the llsn. The log entry contains data.
	Write(llsn types.LLSN, data []byte) error

	WriteBatch(entries []WriteEntry) error

	NewWriteBatch() WriteBatch

	// Commit confirms that the log entry at the llsn is assigned global log position with the
	// glsn.
	Commit(llsn types.LLSN, glsn types.GLSN) error

	CommitBatch(entries []CommitEntry) error

	NewCommitBatch() CommitBatch

	// DeleteCommitted removes committed log entries until the glsn. It acts like garbage collection.
	DeleteCommitted(glsn types.GLSN) error

	// DeleteUncommitted removes uncommitted log entries from the llsn. It should not remove
	// committed log entries. If the log entry at the given llsn is committed, it panics.
	DeleteUncommitted(llsn types.LLSN) error

	// Close closes the storage.
	Close() error
}

type StorageOptions struct {
	Name string
	Path string

	DisableWriteSync  bool
	DisableCommitSync bool

	Logger *zap.Logger
}

type StorageOption func(*StorageOptions)

type initStorageFunc func(*StorageOptions) (Storage, error)

var storages = initStorages()

func initStorages() map[string]initStorageFunc {
	storages := make(map[string]initStorageFunc)
	storages[InMemoryStorageName] = newInMemoryStorage
	storages[PebbleStorageName] = newPebbleStorage
	return storages
}

func ValidStorageName(name string) error {
	if _, ok := storages[name]; !ok {
		return fmt.Errorf("unknown storage %s", name)
	}
	return nil
}

func WithPath(path string) StorageOption {
	return func(opts *StorageOptions) {
		opts.Path = path
	}
}

func WithLogger(logger *zap.Logger) StorageOption {
	return func(opts *StorageOptions) {
		opts.Logger = logger
	}
}

func WithDisableWriteSync() StorageOption {
	return func(opts *StorageOptions) {
		opts.DisableWriteSync = true
	}
}

func WithDisableCommitSync() StorageOption {
	return func(opts *StorageOptions) {
		opts.DisableCommitSync = true
	}
}

func NewStorage(name string, opts ...StorageOption) (Storage, error) {
	if err := ValidStorageName(name); err != nil {
		return nil, err
	}
	options := &StorageOptions{
		Name:   name,
		Logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(options)
	}
	return storages[name](options)
}
