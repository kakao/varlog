package reportcommitter

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storagenode/id"
)

type config struct {
	storageNodeIDGetter   id.StorageNodeIDGetter
	reportCommitterGetter Getter
	logger                *zap.Logger
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

func (c config) validate() error {
	if c.reportCommitterGetter == nil {
		return errors.New("reportCommitterGetter: nil")
	}
	if c.logger == nil {
		return errors.New("logger: nil")
	}
	return nil
}

type Option interface {
	apply(*config)
}

type snidGetterOption struct {
	getter id.StorageNodeIDGetter
}

func (o snidGetterOption) apply(c *config) {
	c.storageNodeIDGetter = o.getter
}

func WithStorageNodeIDGetter(snidGetter id.StorageNodeIDGetter) Option {
	return snidGetterOption{snidGetter}
}

type reportCommitterGetterOption struct {
	rcg Getter
}

func (o reportCommitterGetterOption) apply(c *config) {
	c.reportCommitterGetter = o.rcg
}

func WithReportCommitterGetter(reportCommitterGetter Getter) Option {
	return reportCommitterGetterOption{reportCommitterGetter}
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
