package storage

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/util/stringsutil"
	"github.com/kakao/varlog/pkg/verrors"
)

const (
	DefaultStorageName           = PebbleStorageName
	DefaultWriteSync             = true
	DefaultCommitSync            = true
	DefaultDeleteCommittedSync   = true
	DefaultDeleteUncommittedSync = true
)

type config struct {
	name                  string
	path                  string
	writeSync             bool
	commitSync            bool
	deleteCommittedSync   bool
	deleteUncommittedSync bool
	logger                *zap.Logger
}

func newConfig(opts []Option) (*config, error) {
	cfg := &config{
		name:                  DefaultStorageName,
		writeSync:             DefaultWriteSync,
		commitSync:            DefaultCommitSync,
		deleteCommittedSync:   DefaultDeleteCommittedSync,
		deleteUncommittedSync: DefaultDeleteUncommittedSync,
		logger:                zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c config) validate() error {
	if err := storageNameOption(c.name).validate(); err != nil {
		return err
	}
	if stringsutil.Empty(c.path) {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if c.logger == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	return nil
}

type Option interface {
	apply(*config)
}

type storageNameOption string

func (o storageNameOption) apply(c *config) {
	c.name = string(o)
}

func (o storageNameOption) validate() error {
	if _, ok := storages[string(o)]; !ok {
		return errors.WithStack(verrors.ErrInvalid)
	}
	return nil
}

func WithName(name string) Option {
	return storageNameOption(name)
}

type pathOption string

func (o pathOption) apply(c *config) {
	c.path = string(o)
}

func WithPath(path string) Option {
	return pathOption(path)
}

type writeSyncOption bool

func (o writeSyncOption) apply(c *config) {
	c.writeSync = bool(o)
}

func WithoutWriteSync() Option {
	return writeSyncOption(false)
}

type commitSyncOption bool

func (o commitSyncOption) apply(c *config) {
	c.commitSync = bool(o)
}

func WithoutCommitSync() Option {
	return commitSyncOption(false)
}

type deleteCommittedSyncOption bool

func (o deleteCommittedSyncOption) apply(c *config) {
	c.deleteCommittedSync = bool(o)
}

func WithoutDeleteCommittedSync() Option {
	return deleteCommittedSyncOption(false)
}

type deleteUncommittedSyncOption bool

func (o deleteUncommittedSyncOption) apply(c *config) {
	c.deleteUncommittedSync = bool(o)
}

func WithoutDeleteUncommittedSync() Option {
	return deleteUncommittedSyncOption(false)
}

type loggerOption struct {
	logger *zap.Logger
}

func (o loggerOption) apply(c *config) {
	c.logger = o.logger
}

func WithLogger(logger *zap.Logger) Option {
	return loggerOption{logger}
}
