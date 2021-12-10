package storage

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode/storage -package storage -destination storage_mock.go . Scanner,WriteBatch,CommitBatch,Storage

import (
	"errors"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

var (
	ErrNotFoundCommitContext     = errors.New("storage: no commit context")
	ErrInconsistentCommitContext = errors.New("storage: inconsistent commit context")
	ErrInvalidScanRange          = errors.New("storage: invalid scan range")
)

type RecoveryInfo struct {
	LastCommitContext struct {
		CC    CommitContext
		Found bool
	}
	LastNonEmptyCommitContext struct {
		CC    CommitContext
		Found bool
	}
	LogEntryBoundary struct {
		First varlogpb.LogEntryMeta
		Last  varlogpb.LogEntryMeta
		Found bool
	}
	UncommittedLogEntryBoundary struct {
		First types.LLSN
		Last  types.LLSN
	}
}

type scanConfig struct {
	withGLSN bool
	begin    varlogpb.LogEntryMeta
	end      varlogpb.LogEntryMeta
}

func newScanConfig(opts []ScanOption) (scanConfig, error) {
	cfg := scanConfig{}
	for _, opt := range opts {
		opt.applyScan(&cfg)
	}
	return cfg, cfg.validate()
}

func (cfg scanConfig) validate() error {
	if cfg.withGLSN && cfg.begin.GLSN >= cfg.end.GLSN {
		return ErrInvalidScanRange
	}
	if !cfg.withGLSN && cfg.begin.LLSN >= cfg.end.LLSN {
		return ErrInvalidScanRange
	}
	return nil
}

type ScanOption interface {
	applyScan(*scanConfig)
}

type scanRangeOption struct {
	withGLSN bool
	begin    varlogpb.LogEntryMeta
	end      varlogpb.LogEntryMeta
}

func (opt scanRangeOption) applyScan(cfg *scanConfig) {
	cfg.withGLSN = opt.withGLSN
	cfg.begin = opt.begin
	cfg.end = opt.end
}

func WithGLSN(begin types.GLSN, end types.GLSN) ScanOption {
	return scanRangeOption{
		withGLSN: true,
		begin:    varlogpb.LogEntryMeta{GLSN: begin},
		end:      varlogpb.LogEntryMeta{GLSN: end},
	}
}

func WithLLSN(begin types.LLSN, end types.LLSN) ScanOption {
	return scanRangeOption{
		withGLSN: false,
		begin:    varlogpb.LogEntryMeta{LLSN: begin},
		end:      varlogpb.LogEntryMeta{LLSN: end},
	}
}

// ScanResult represents a result of Scanner.Next() method. It should be immutable.
type ScanResult struct {
	LogEntry varlogpb.LogEntry
	Err      error
}

func NewInvalidScanResult(err error) ScanResult {
	return ScanResult{
		LogEntry: varlogpb.InvalidLogEntry(),
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

var InvalidCommitContext = CommitContext{
	Version:            types.InvalidVersion,
	CommittedGLSNBegin: types.InvalidGLSN,
	CommittedGLSNEnd:   types.InvalidGLSN,
}

type CommitContext struct {
	Version            types.Version
	HighWatermark      types.GLSN
	CommittedGLSNBegin types.GLSN
	CommittedGLSNEnd   types.GLSN
	CommittedLLSNBegin types.LLSN
}

func (cc CommitContext) Empty() bool {
	if cc.CommittedGLSNEnd < cc.CommittedGLSNBegin {
		panic("invalid commit context")
	}
	return cc.CommittedGLSNEnd-cc.CommittedGLSNBegin == 0
}

func (cc CommitContext) Equal(other CommitContext) bool {
	return cc.Version == other.Version &&
		cc.CommittedGLSNBegin == other.CommittedGLSNBegin &&
		cc.CommittedGLSNEnd == other.CommittedGLSNEnd &&
		cc.CommittedLLSNBegin == other.CommittedLLSNBegin
}

type Storage interface {
	// Name returns the storage unique name.
	Name() string

	// Path returns directory to store storage data files.
	Path() string

	// Read reads the log entry at the glsn.
	// If there is no entry at the given position, it returns varlog.ErrNoEntry.
	Read(glsn types.GLSN) (varlogpb.LogEntry, error)

	// ReadGE reads the closest log entry located at greater than or equal to the argument glsn.
	// If there is no log entry at the proper position, it returns varlog.ErrNoEntry.
	ReadGE(glsn types.GLSN) (varlogpb.LogEntry, error)

	// ReadAt reads the log entry at the llsn.
	ReadAt(llsn types.LLSN) (varlogpb.LogEntry, error)

	// Scan returns Scanner that reads log entries from the glsn.
	Scan(opts ...ScanOption) Scanner

	// Write writes log entry at the llsn. The log entry contains data.
	// Write(llsn types.LLSN, data []byte) error

	// NewWriteBatch creates a batch for write operations.
	NewWriteBatch() WriteBatch

	// Commit confirms that the log entry at the llsn is assigned global log position with the
	// glsn.
	// Commit(llsn types.LLSN, glsn types.GLSN) error

	// NewCommitBatch creates a batch for commit operations.
	NewCommitBatch(commitContext CommitContext) (CommitBatch, error)

	// ReadFloorCommitContext returns a commit context whose member prevHighWatermark is the
	// greatest commit context less than or equal to the given parameter prevHighWatermark.
	ReadFloorCommitContext(ver types.Version) (CommitContext, error)

	// CommitContextOf looks up a commit context that contains the log entry positioned at the
	// given glsn.
	CommitContextOf(glsn types.GLSN) (CommitContext, error)

	NextCommitContextOf(commitContext CommitContext) (CommitContext, error)

	// RestoreLogStreamContext restores the LogStreamContext that can be recovered by contents
	// of the storage. The LogStreamContext referred to by the parameter is filled with restored
	// context. If the LogStreamContext is recovered, the RestoreLogStreamContext returns true,
	// otherwise false.
	//RestoreLogStreamContext(lsc *logstream.LogStreamContext) bool

	ReadRecoveryInfo() (RecoveryInfo, error)

	// RestoreStorage restores the status of storage.
	RestoreStorage(lastWrittenLLSN types.LLSN, lastCommittedLLSN types.LLSN, lastCommittedGLSN types.GLSN)

	// StoreCommitContext writes context information to storage when a group of logs are
	// committed. It must be called ahead of commits that are requested by a metadata
	// repository. If it fails, commits requested by the metadata repository should not be
	// written.
	// StoreCommitContext(commitContext CommitContext) error

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

type initStorageFunc func(*config) (Storage, error)

var storages = initStorages()

func initStorages() map[string]initStorageFunc {
	storages := make(map[string]initStorageFunc)
	// storages[InMemoryStorageName] = newInMemoryStorage
	storages[PebbleStorageName] = newPebbleStorage
	return storages
}

func NewStorage(opts ...Option) (Storage, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	return storages[cfg.name](cfg)
}
