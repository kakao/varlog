package storage

import (
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

type logAdaptor struct {
	logger *zap.SugaredLogger
}

var _ pebble.Logger = (*logAdaptor)(nil)

func newLogAdaptor(logger *zap.Logger) *logAdaptor {
	return &logAdaptor{
		logger: logger.Sugar(),
	}
}

func (l *logAdaptor) Infof(format string, args ...interface{}) {
	l.logger.Infof(format, args...)
}

func (l *logAdaptor) Fatalf(format string, args ...interface{}) {
	l.logger.Fatalf(format, args...)
}
