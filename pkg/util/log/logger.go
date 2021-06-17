package log

import (
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Level = zapcore.Level

func New(opts ...Option) (*zap.Logger, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	// WriteSyncer
	var syncer zapcore.WriteSyncer
	if !cfg.disableLogToStderr {
		syncer = zapcore.Lock(zapcore.AddSync(os.Stderr))
	}
	if len(cfg.path) > 0 {
		fileSyncer := zapcore.AddSync(&lumberjack.Logger{
			Filename:   cfg.path,
			LocalTime:  cfg.localTime,
			Compress:   cfg.compress,
			MaxSize:    cfg.maxSizeMB,
			MaxBackups: cfg.maxBackups,
			MaxAge:     cfg.maxAgeDays,
		})
		if syncer != nil {
			syncer = zap.CombineWriteSyncers(syncer, fileSyncer)
		} else {
			syncer = fileSyncer
		}
	}

	// Encoder
	var encoder zapcore.Encoder
	if cfg.humanFriendly {
		encoder = zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	} else {
		encoder = zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	}

	// Level
	level := zap.NewAtomicLevelAt(cfg.level)

	// Core
	core := zapcore.NewCore(encoder, syncer, level)

	// Recommended zap.Option
	zapOpts := append(cfg.zapOpts,
		zap.AddCaller(),
		zap.AddStacktrace(zap.ErrorLevel),
	)

	// Sampler
	if cfg.zapSampling != nil {
		zapOpts = append(zapOpts, zap.WrapCore(func(zapcore.Core) zapcore.Core {
			var samplerOpts []zapcore.SamplerOption
			if cfg.zapSampling.Hook != nil {
				samplerOpts = append(samplerOpts, zapcore.SamplerHook(cfg.zapSampling.Hook))
			}
			return zapcore.NewSamplerWithOptions(
				core,
				time.Second,
				cfg.zapSampling.Initial,
				cfg.zapSampling.Thereafter,
				samplerOpts...,
			)
		}))
	}

	logger := zap.New(core, zapOpts...)
	return logger, nil
}
