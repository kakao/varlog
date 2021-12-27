package logio

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/id"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/telemetry"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

type config struct {
	storageNodeIDGetter id.StorageNodeIDGetter
	readWriterGetter    Getter
	metrics             *telemetry.Metrics
	logger              *zap.Logger
}

func newConfig(opts []Option) config {
	cfg := config{
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		panic(err)
	}
	return cfg
}

func (c *config) validate() error {
	if c.storageNodeIDGetter == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if c.readWriterGetter == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if c.metrics == nil {
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

type snidGetterOption struct {
	id.StorageNodeIDGetter
}

func (o snidGetterOption) apply(c *config) {
	c.storageNodeIDGetter = o.StorageNodeIDGetter
}

func WithStorageNodeIDGetter(snidGetter id.StorageNodeIDGetter) Option {
	return snidGetterOption{snidGetter}
}

type readWriterGetterOption struct {
	getter Getter
}

func (o readWriterGetterOption) apply(c *config) {
	c.readWriterGetter = o.getter
}

func WithReadWriterGetter(getter Getter) Option {
	return readWriterGetterOption{getter}
}

type measurableOption struct {
	m *telemetry.Metrics
}

func (o measurableOption) apply(c *config) {
	c.metrics = o.m
}

func WithMetrics(measurable *telemetry.Metrics) Option {
	return measurableOption{measurable}
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
