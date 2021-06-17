package log

import (
	"errors"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.daumkakao.com/varlog/varlog/pkg/util/fputil"
)

const (
	defaultLogLevel = zapcore.InfoLevel

	defaultMaxSizeMB  = 100 // 100MB
	defaultMaxAgeDays = 0   // retain all
	defaultMaxBackups = 0   // retain all
	defaultLogDirMode = os.FileMode(0755)
)

type config struct {
	disableLogToStderr bool

	humanFriendly bool
	level         Level
	zapOpts       []zap.Option
	zapSampling   *zap.SamplingConfig

	// log rotate
	path       string
	maxSizeMB  int
	maxAgeDays int
	maxBackups int
	compress   bool
	localTime  bool
	logDirMode os.FileMode
}

func newConfig(opts []Option) (cfg config, err error) {
	cfg = config{
		level:      defaultLogLevel,
		maxSizeMB:  defaultMaxSizeMB,
		maxAgeDays: defaultMaxAgeDays,
		maxBackups: defaultMaxBackups,
		logDirMode: defaultLogDirMode,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	err = cfg.validate()
	return cfg, err
}

func (c config) validate() error {
	if len(c.path) > 0 {
		if c.path[len(c.path)-1] == '/' {
			return errors.New("logger: invalid file path")
		}
		if err := os.MkdirAll(filepath.Dir(c.path), c.logDirMode); err != nil {
			return err
		}
		if err := fputil.IsWritableDir(filepath.Dir(c.path)); err != nil {
			return err
		}
	}
	return nil
}

type Option func(*config)

func WithoutLogToStderr() Option {
	return func(c *config) {
		c.disableLogToStderr = true
	}
}

func WithPath(path string) Option {
	return func(c *config) {
		c.path = path
	}
}

func WithMaxSizeMB(maxSizeMB int) Option {
	return func(c *config) {
		c.maxSizeMB = maxSizeMB
	}
}

func WithAgeDays(maxAgeDays int) Option {
	return func(c *config) {
		c.maxAgeDays = maxAgeDays
	}
}

func WithMaxBackups(maxBackups int) Option {
	return func(c *config) {
		c.maxBackups = maxBackups
	}
}

func WithLocalTime() Option {
	return func(c *config) {
		c.localTime = true
	}
}

func WithCompression() Option {
	return func(c *config) {
		c.compress = true
	}
}

func WithLogDirMode(mode os.FileMode) Option {
	return func(c *config) {
		c.logDirMode = mode
	}
}

func WithHumanFriendly() Option {
	return func(c *config) {
		c.humanFriendly = true
	}
}

func WithLogLevel(level Level) Option {
	return func(c *config) {
		c.level = level
	}
}

func WithZapLoggerOptions(opts ...zap.Option) Option {
	return func(c *config) {
		c.zapOpts = opts
	}
}

func WithZapSampling(zapSampling *zap.SamplingConfig) Option {
	return func(c *config) {
		c.zapSampling = zapSampling
	}
}
