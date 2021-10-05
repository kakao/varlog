package log

import (
	"errors"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/kakao/varlog/pkg/util/fputil"
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

// Option configures a logger.
type Option func(*config)

// WithoutLogToStderr disalbes logging to stderr.
func WithoutLogToStderr() Option {
	return func(c *config) {
		c.disableLogToStderr = true
	}
}

// WithPath sets a path of the log file.
// If this option isn't set, logging to a file will be disabled.
func WithPath(path string) Option {
	return func(c *config) {
		c.path = path
	}
}

// WithMaxSizeMB sets the maximum size of a log file in megabytes.
// If a log file increases more than the maxSizeMB, it will be rotated.
// Its default value is 100 megabytes.
func WithMaxSizeMB(maxSizeMB int) Option {
	return func(c *config) {
		c.maxSizeMB = maxSizeMB
	}
}

// WithAgeDays sets the retention of log files in days.
// There is no retention rule based on age if this option is not set.
func WithAgeDays(maxAgeDays int) Option {
	return func(c *config) {
		c.maxAgeDays = maxAgeDays
	}
}

// WithMaxBackups sets the maximum number of files to retain.
// There is no retention rule based on the number of files if this option is not set.
func WithMaxBackups(maxBackups int) Option {
	return func(c *config) {
		c.maxBackups = maxBackups
	}
}

// WithLocalTime sets the local time to the timestamp in log file names.
// UTC will be used if this option is not set.
func WithLocalTime() Option {
	return func(c *config) {
		c.localTime = true
	}
}

// WithCompression makes the old log files compressed.
// Old files are not compressed if this option is not set.
func WithCompression() Option {
	return func(c *config) {
		c.compress = true
	}
}

// WithLogDirMode sets mode and permission of log directory.
// Its default value is 0755.
func WithLogDirMode(mode os.FileMode) Option {
	return func(c *config) {
		c.logDirMode = mode
	}
}

// WithHumanFriendly makes the logger print logs in a human-friendly format.
// This option prints logs in text format rather than JSON and times in ISO8601 format rather than
// UNIX time format.
func WithHumanFriendly() Option {
	return func(c *config) {
		c.humanFriendly = true
	}
}

// WithLogLevel sets log level.
// The default value is INFO.
func WithLogLevel(level Level) Option {
	return func(c *config) {
		c.level = level
	}
}

// WithZapLoggerOptions sets options for zap logger.
func WithZapLoggerOptions(opts ...zap.Option) Option {
	return func(c *config) {
		c.zapOpts = opts
	}
}

// WithZapSampling sets sampling options for zap logger.
func WithZapSampling(zapSampling *zap.SamplingConfig) Option {
	return func(c *config) {
		c.zapSampling = zapSampling
	}
}
