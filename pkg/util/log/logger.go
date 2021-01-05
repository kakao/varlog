package log

import (
	"context"
	"runtime"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	logCallDepth  = 1
	attrCallDepth = logCallDepth + 1

	packageSeparator = '/'
	lineSeparator    = ':'

	loggerNameAttributeKey  = "logger"
	loggerLevelAttributeKey = "level"
	callerAttributeKey      = "caller"
)

var (
	codeFilepathAttributeKey = label.Key("code.filepath")
	codeLinenoAtrributeKey   = label.Key("code.lineno")
)

var (
	debugLabel  = label.String(loggerLevelAttributeKey, "debug")
	infoLabel   = label.String(loggerLevelAttributeKey, "info")
	warnLabel   = label.String(loggerLevelAttributeKey, "warn")
	errorLabel  = label.String(loggerLevelAttributeKey, "error")
	dpanicLabel = label.String(loggerLevelAttributeKey, "dpanic")
	panicLabel  = label.String(loggerLevelAttributeKey, "panic")
	fatalLabel  = label.String(loggerLevelAttributeKey, "fatal")
)

type Logger struct {
	z        *zap.Logger
	nameAttr label.KeyValue
	attrs    []label.KeyValue
}

func New(opts Options) (*Logger, error) {
	zlogger, err := NewInternal(opts)
	if err != nil {
		return nil, err
	}
	zlogger = zlogger.WithOptions(zap.AddCallerSkip(logCallDepth))
	return &Logger{z: zlogger}, nil
}

func (log *Logger) Sync() error {
	// TODO: flush opentelemetry
	return log.z.Sync()
}

func (log *Logger) Named(name string) *Logger {
	if name == "" {
		return log
	}
	oldName := log.nameAttr.Value.AsString()
	var newName string
	if oldName == "" {
		newName = name
	} else {
		newName = strings.Join([]string{oldName, name}, ".")
	}
	return &Logger{z: log.z.Named(name), nameAttr: label.String(loggerNameAttributeKey, newName)}
}

func (log *Logger) With(fields ...zap.Field) *Logger {
	newAttrs := make([]label.KeyValue, 0, len(log.attrs)+len(fields))
	copy(newAttrs, log.attrs)
	newAttrs = append(newAttrs, fieldsToAttributes(fields...)...)
	return &Logger{z: log.z.With(fields...), nameAttr: log.nameAttr, attrs: newAttrs}
}

func (log *Logger) Debug(msg string, fields ...zap.Field) {
	log.z.Debug(msg, fields...)
}

func (log *Logger) DebugC(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, debugLabel, fields...)
	log.z.Debug(msg, fields...)
}

func (log *Logger) Info(msg string, fields ...zap.Field) {
	log.z.Info(msg, fields...)
}

func (log *Logger) InfoWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, infoLabel, fields...)
	log.z.Info(msg, fields...)
}

func (log *Logger) Warn(msg string, fields ...zap.Field) {
	log.z.Warn(msg, fields...)
}

func (log *Logger) WarnWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, warnLabel, fields...)
	log.z.Warn(msg, fields...)
}

func (log *Logger) Error(msg string, fields ...zap.Field) {
	log.z.Error(msg, fields...)
}

func (log *Logger) ErrorWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, errorLabel, fields...)
	log.z.Error(msg, fields...)
}

func (log *Logger) DPanic(msg string, fields ...zap.Field) {
	log.z.DPanic(msg, fields...)
}

func (log *Logger) DPanicWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, dpanicLabel, fields...)
	log.z.DPanic(msg, fields...)
}

func (log *Logger) Panic(msg string, fields ...zap.Field) {
	log.z.Panic(msg, fields...)
}

func (log *Logger) PanicWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, panicLabel, fields...)
	log.z.Panic(msg, fields...)
}

func (log *Logger) Fatal(msg string, fields ...zap.Field) {
	log.z.Fatal(msg, fields...)
}

func (log *Logger) FatalWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, fatalLabel, fields...)
	log.z.Fatal(msg, fields...)
}

func (log *Logger) setAttributesToSpan(ctx context.Context, msg string, lvl label.KeyValue, fields ...zap.Field) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		attrs := make([]label.KeyValue, 0, len(log.attrs)+len(fields)+2)
		// TODO: use Severity in OTEL
		attrs = append(attrs, lvl)
		if _, file, line, ok := runtime.Caller(attrCallDepth); ok {
			attrs = append(attrs, codeFilepathAttributeKey.String(file), codeLinenoAtrributeKey.Int(line))
			// attrs = append(attrs, label.String(callerAttributeKey, getCaller(file, line)))
		}
		attrs = append(attrs, log.attrs...)
		attrs = appendAttributesByFields(attrs, fields...)
		span.AddEvent(msg, trace.WithAttributes(attrs...))
	}
}

func getCaller(file string, line int) string {
	pos := strings.LastIndexByte(file, packageSeparator)
	if pos == -1 {
		return file
	}
	pos = strings.LastIndexByte(file[:pos], packageSeparator)
	if pos == -1 {
		return file
	}
	var b strings.Builder
	b.WriteString(file[pos+1:])
	b.WriteByte(lineSeparator)
	b.WriteString(strconv.Itoa(line))
	return b.String()
}
