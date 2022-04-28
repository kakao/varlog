package metarepos

import (
	"go.uber.org/zap"
)

type RaftLogger struct {
	*zap.SugaredLogger
}

func NewRaftLogger(logger *zap.Logger) *RaftLogger {
	return &RaftLogger{logger.Sugar()}
}

func (l *RaftLogger) Warning(v ...interface{}) {
	l.Warn(v...)
}

func (l *RaftLogger) Warningf(template string, args ...interface{}) {
	l.Warnf(template, args...)
}
