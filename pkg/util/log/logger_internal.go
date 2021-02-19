package log

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/kakao/varlog/pkg/util/fputil"
)

const (
	DefaultMaxSizeMB  = 100
	DefaultMaxAgeDay  = 30
	DefaultMaxBackups = 100

	defaultSamplerTick       = time.Second
	defaultSamplerFirst      = 100
	defaultSamplerThereafter = 100

	logDirMode = os.FileMode(0755)
)

type Options struct {
	RotateOptions
	Sampler *SamplerOptions

	DisableLogToStderr bool
	Path               string
	Debug              bool
}

type SamplerOptions struct {
	Tick       time.Duration
	First      int
	Thereafter int
}

type RotateOptions struct {
	MaxSizeMB  int
	MaxAgeDays int
	MaxBackups int
	Compress   bool
	LocalTime  bool
}

func defaultProductionSamplerOption() *SamplerOptions {
	return &SamplerOptions{
		Tick:       defaultSamplerTick,
		First:      defaultSamplerFirst,
		Thereafter: defaultSamplerThereafter,
	}
}

func NewInternal(opts Options) (*zap.Logger, error) {
	if opts.DisableLogToStderr && len(opts.Path) == 0 {
		return nil, errors.New("logger: invalid argument")
	}

	var writeSyncer zapcore.WriteSyncer
	if !opts.DisableLogToStderr {
		writeSyncer = zapcore.Lock(zapcore.AddSync(os.Stderr))
	}
	if len(opts.Path) > 0 {
		if opts.Path[len(opts.Path)-1] == '/' {
			return nil, errors.New("logger: invalid file path")
		}

		if err := os.MkdirAll(filepath.Dir(opts.Path), logDirMode); err != nil {
			return nil, err
		}
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
		if writeSyncer != nil {
			writeSyncer = zap.CombineWriteSyncers(writeSyncer, fileSyncer)
		} else {
			writeSyncer = fileSyncer
		}
	}

	var (
		lvl     zap.AtomicLevel
		encoder zapcore.Encoder
	)
	if opts.Debug {
		lvl = zap.NewAtomicLevelAt(zap.DebugLevel)
		encoder = zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	} else {
		lvl = zap.NewAtomicLevelAt(zap.InfoLevel)
		encoder = zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	}
	core := zapcore.NewCore(encoder, writeSyncer, lvl)

	zapOpts := []zap.Option{zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel)}
	var samplerOptions *SamplerOptions
	if opts.Sampler != nil {
		samplerOptions = opts.Sampler
	}
	if opts.Debug {
		zapOpts = append(zapOpts, zap.Development())
		//} else if samplerOptions == nil {
		//	samplerOptions = defaultProductionSamplerOption()
	}
	if samplerOptions != nil {
		zapOpts = append(zapOpts, zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return zapcore.NewSamplerWithOptions(core, samplerOptions.Tick, samplerOptions.First, samplerOptions.Thereafter)
		}))
	}

	logger := zap.New(core, zapOpts...)
	return logger, nil
}
