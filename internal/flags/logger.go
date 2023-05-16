package flags

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"
	"go.uber.org/zap/zapcore"

	"github.com/kakao/varlog/pkg/util/log"
)

const (
	CategoryLogger = "Logger:"

	DefaultLogFileMaxBackups    = 100
	DefaultLogFileRetentionDays = 14
	DefaultLogFileMaxSizeMB     = 100
	DefaultLogLevel             = "INFO"
)

var (
	// LogDir is a flag specifying the directory of the logs.
	LogDir = &cli.StringFlag{
		Name:     "logdir",
		Category: CategoryLogger,
		Aliases:  []string{"log-dir"},
		EnvVars:  []string{"LOGDIR", "LOG_DIR"},
		Usage:    "Directory for the log files.",
	}
	// LogToStderr is a flag that decides whether the logs are printed to the stderr.
	LogToStderr = &cli.BoolFlag{
		Name:     "logtostderr",
		Category: CategoryLogger,
		Aliases:  []string{"log-to-stderr"},
		EnvVars:  []string{"LOGTOSTDERR"},
		Usage:    "Print the logs to the stderr.",
	}
	// LogFileMaxBackups is a flag specifying the maximum number of backup log files.
	LogFileMaxBackups = &cli.IntFlag{
		Name:     "logfile-max-backups",
		Category: CategoryLogger,
		EnvVars:  []string{"LOGFILE_MAX_BACKUPS"},
		Value:    DefaultLogFileMaxBackups,
		Usage:    "Maximum number of backup log files. Retain all backup files if zero.",
		Action: func(_ *cli.Context, value int) error {
			if value < 0 {
				return fmt.Errorf("invalid value \"%d\" for flag --logfile-max-backups", value)
			}
			return nil
		},
	}

	// LogFileRetentionDays is a flag specifying the age of backup log files.
	LogFileRetentionDays = &cli.IntFlag{
		Name:     "logfile-retention-days",
		Category: CategoryLogger,
		EnvVars:  []string{"LOGFILE_RETENTION_DAYS"},
		Value:    DefaultLogFileRetentionDays,
		Usage:    "Age of backup log files. Unlimited age if zero, that is, retain all.",
		Action: func(_ *cli.Context, value int) error {
			if value < 0 {
				return fmt.Errorf("invalid value \"%d\" for flag --logfile-retention-days", value)
			}
			return nil
		},
	}
	// LogFileMaxSizeMB is a flag specifying the maximum size for each log file.
	LogFileMaxSizeMB = &cli.IntFlag{
		Name:     "logfile-max-size-mb",
		Category: CategoryLogger,
		EnvVars:  []string{"LOGFILE_MAX_SIZE_MB"},
		Value:    DefaultLogFileMaxSizeMB,
		Usage:    "Maximum file size for each log file.",
		Action: func(_ *cli.Context, value int) error {
			if value <= 0 {
				return fmt.Errorf("invalid value \"%d\" for flag --logfile-max-size-mb", value)
			}
			return nil
		},
	}
	// LogFileNameUTC is a flag that decides whether backup log files are named with timestamps in UTC.
	LogFileNameUTC = &cli.BoolFlag{
		Name:     "logfile-name-utc",
		Category: CategoryLogger,
		EnvVars:  []string{"LOGFILE_NAME_UTC"},
		Usage:    "Whether backup log files are named with timestamps in UTC or local time if not set. Log files are named 'example-1970-01-01T00-00-00.000.log' when the file is rotated.",
	}

	// LogFIleCompression is a flag that decides whether backup log files are compressed.
	LogFileCompression = &cli.BoolFlag{
		Name:     "logfile-compression",
		Category: CategoryLogger,
		EnvVars:  []string{"LOGFILE_COMPRESSION"},
		Usage:    "Compress backup log files.",
	}

	// LogHumanReadable is a flag that decides whether logs are human-readable.
	LogHumanReadable = &cli.BoolFlag{
		Name:     "log-human-readable",
		Category: CategoryLogger,
		EnvVars:  []string{"LOG_HUMAN_READABLE"},
		Usage:    "Human-readable output.",
	}

	// LogLevel is a flag specifying log level.
	LogLevel = &cli.StringFlag{
		Name:     "loglevel",
		Category: CategoryLogger,
		Aliases:  []string{"log-level"},
		EnvVars:  []string{"LOGLEVEL", "LOG_LEVEL"},
		Value:    DefaultLogLevel,
		Usage:    "Log levels, either debug, info, warn, or error case-insensitively.",
	}
)

func ParseLoggerFlags(c *cli.Context, maybeLogFileName string) (opts []log.Option, err error) {
	opts = []log.Option{
		log.WithMaxBackups(c.Int(LogFileMaxBackups.Name)),
		log.WithAgeDays(c.Int(LogFileRetentionDays.Name)),
		log.WithMaxSizeMB(c.Int(LogFileMaxSizeMB.Name)),
	}

	if logDir := c.String(LogDir.Name); len(logDir) != 0 {
		logDir, err = filepath.Abs(logDir)
		if err != nil {
			return nil, err
		}
		opts = append(opts, log.WithPath(filepath.Join(logDir, maybeLogFileName)))
	}

	if !c.Bool(LogToStderr.Name) {
		opts = append(opts, log.WithoutLogToStderr())
	}

	if !c.Bool(LogFileNameUTC.Name) {
		opts = append(opts, log.WithLocalTime())
	}

	if c.Bool(LogFileCompression.Name) {
		opts = append(opts, log.WithCompression())
	}

	if c.Bool(LogHumanReadable.Name) {
		opts = append(opts, log.WithHumanFriendly())
	}

	level, err := zapcore.ParseLevel(strings.ToLower(c.String(LogLevel.Name)))
	if err != nil {
		return nil, err
	}
	opts = append(opts, log.WithLogLevel(level))

	return opts, nil
}
