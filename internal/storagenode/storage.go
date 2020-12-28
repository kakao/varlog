package storagenode

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode -package storagenode -destination storage_mock.go . Scanner,WriteBatch,CommitBatch,Storage

import (
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
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

type WriteBatch interface {
	Put(llsn types.LLSN, data []byte) error
	Apply() error
	Close() error
}

type CommitBatch interface {
	Put(llsn types.LLSN, glsn types.GLSN) error
	Apply() error
	Close() error
}

type CommitContext struct {
	HighWatermark      types.GLSN
	PrevHighWatermark  types.GLSN
	CommittedGLSNBegin types.GLSN
	CommittedGLSNEnd   types.GLSN
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

	// NewWriteBatch creates a batch for write operations.
	NewWriteBatch() WriteBatch

	// Commit confirms that the log entry at the llsn is assigned global log position with the
	// glsn.
	Commit(llsn types.LLSN, glsn types.GLSN) error

	// NewCommitBatch creates a batch for commit operations.
	NewCommitBatch() CommitBatch

	// RestoreLogStreamContext restores the LogStreamContext that can be recovered by contents
	// of the storage. The LogStreamContext referred to by the parameter is filled with restored
	// context. If the LogStreamContext is recovered, the RestoreLogStreamContext returns true,
	// otherwise false.
	RestoreLogStreamContext(lsc *LogStreamContext) bool

	// RestoreStorage restores the status of storage.
	RestoreStorage(lastLLSN types.LLSN, lastGLSN types.GLSN)

	// StoreCommitContext writes context information to storage when a group of logs are
	// committed. It must be called ahead of commits that are requested by a metadata
	// repository. If it fails, commits requested by the metadata repository should not be
	// written.
	StoreCommitContext(commitContext CommitContext) error

	// DeleteCommitted removes committed log entries until the glsn. It acts like garbage collection.
	// If prefixEnd is invalid, DeleteCommitted returns an error. If it tries to delete
	// uncommitted logs, it returns an error.
	DeleteCommitted(prefixEnd types.GLSN) error

	// DeleteUncommitted removes uncommitted log entries from the llsn. It should not remove
	// committed log entries. If the log entry at the given llsn is committed, it panics.
	// If the storage is empty, that is, it has no written logs, DeleteUncommitted returns nil.
	DeleteUncommitted(suffixBegin types.LLSN) error

	// Close closes the storage.
	Close() error
}

const (
	DefaultStorageName                   = PebbleStorageName
	DefaultEnableWriteFsync              = false // default: no sync
	DefaultEnableCommitFsync             = false // default: no sync
	DefaultEnableCommitContextFsync      = false // default: no sync
	DefaultEnableDeleteCommittedFsync    = false // default: no sync
	DefaultDisableDeleteUncommittedFsync = false // default: sync
)

type StorageOptions struct {
	Name string
	Path string

	EnableWriteFsync              bool
	EnableCommitFsync             bool
	EnableCommitContextFsync      bool
	EnableDeleteCommittedFsync    bool
	DisableDeleteUncommittedFsync bool

	Logger *zap.Logger
}

func DefaultStorageOptions() StorageOptions {
	return StorageOptions{
		Name:                          DefaultStorageName,
		EnableWriteFsync:              DefaultEnableWriteFsync,
		EnableCommitFsync:             DefaultEnableCommitFsync,
		EnableCommitContextFsync:      DefaultEnableCommitContextFsync,
		EnableDeleteCommittedFsync:    DefaultEnableDeleteCommittedFsync,
		DisableDeleteUncommittedFsync: DefaultDisableDeleteUncommittedFsync,
		Logger:                        zap.NewNop(),
	}
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

func WithEnableWriteFsync() StorageOption {
	return func(opts *StorageOptions) {
		opts.EnableWriteFsync = true
	}
}

func WithEnableCommitFsync() StorageOption {
	return func(opts *StorageOptions) {
		opts.EnableCommitFsync = true
	}
}

func WithEnableCommitContextFsync() StorageOption {
	return func(opts *StorageOptions) {
		opts.EnableCommitContextFsync = true
	}
}

func WithEnableDeleteCommittedFsync() StorageOption {
	return func(opts *StorageOptions) {
		opts.EnableDeleteCommittedFsync = true
	}
}

func WithDisableDeleteUncommittedFsync() StorageOption {
	return func(opts *StorageOptions) {
		opts.DisableDeleteUncommittedFsync = true
	}
}

func NewStorage(name string, opts ...StorageOption) (Storage, error) {
	if err := ValidStorageName(name); err != nil {
		return nil, err
	}
	options := DefaultStorageOptions()
	options.Name = name
	for _, opt := range opts {
		opt(&options)
	}
	return storages[name](&options)
}
