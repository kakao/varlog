package loggerutil

import (
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/kakao/varlog/pkg/util/fputil"
)

const (
	DefaultMaxSizeMB  = 100
	DefaultMaxAgeDay  = 30
	DefaultMaxBackups = 100
)

type Options struct {
	RotateOptions

	Path  string
	Debug bool
}

type RotateOptions struct {
	MaxSizeMB  int
	MaxAgeDays int
	MaxBackups int
	Compress   bool
	LocalTime  bool
}

func New(opts Options) (*zap.Logger, error) {
	writerSyncer := zapcore.AddSync(os.Stderr)
	if len(opts.Path) > 0 {
		if err := fputil.IsWritableDir(filepath.Dir(opts.Path)); err != nil {
			return nil, err
		}
		fileSyncer := zapcore.AddSync(&lumberjack.Logger{
			Filename:   opts.Path,
			LocalTime:  opts.LocalTime,
			Compress:   opts.Compress,
			MaxSize:    opts.MaxSizeMB,
			MaxBackups: opts.MaxBackups,
			MaxAge:     opts.MaxAgeDays,
		})
		writerSyncer = zap.CombineWriteSyncers(writerSyncer, fileSyncer)
	}

	var encoder zapcore.Encoder
	if opts.Debug {
		encoder = zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())

	} else {
		encoder = zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	}

	core := zapcore.NewCore(
		encoder,
		writerSyncer,
		zap.InfoLevel,
	)
	var zapOpts []zap.Option
	if opts.Debug {
		zapOpts = append(zapOpts, zap.Development())
	}
	logger := zap.New(core, zapOpts...)
	return logger, nil
}
