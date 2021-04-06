package log

import (
	"context"
	"runtime"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	codeFilepathAttributeKey = attribute.Key("code.filepath")
	codeLinenoAtrributeKey   = attribute.Key("code.lineno")
)

var (
	debugattribute  = attribute.String(loggerLevelAttributeKey, "debug")
	infoattribute   = attribute.String(loggerLevelAttributeKey, "info")
	warnattribute   = attribute.String(loggerLevelAttributeKey, "warn")
	errorattribute  = attribute.String(loggerLevelAttributeKey, "error")
	dpanicattribute = attribute.String(loggerLevelAttributeKey, "dpanic")
	panicattribute  = attribute.String(loggerLevelAttributeKey, "panic")
	fatalattribute  = attribute.String(loggerLevelAttributeKey, "fatal")
)

type Level = zapcore.Level

type Logger struct {
	z        *zap.Logger
	nameAttr attribute.KeyValue
	attrs    []attribute.KeyValue
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
	return &Logger{z: log.z.Named(name), nameAttr: attribute.String(loggerNameAttributeKey, newName)}
}

func (log *Logger) With(fields ...zap.Field) *Logger {
	newAttrs := make([]attribute.KeyValue, 0, len(log.attrs)+len(fields))
	copy(newAttrs, log.attrs)
	newAttrs = append(newAttrs, fieldsToAttributes(fields...)...)
	return &Logger{z: log.z.With(fields...), nameAttr: log.nameAttr, attrs: newAttrs}
}

func (log *Logger) Enabled(lvl zapcore.Level) bool {
	return log.z.Core().Enabled(lvl)
}

func (log *Logger) DebugEnabled() bool {
	return log.Enabled(zapcore.DebugLevel)
}

func (log *Logger) Debug(msg string, fields ...zap.Field) {
	log.z.Debug(msg, fields...)
}

func (log *Logger) DebugWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, debugattribute, fields...)
	log.z.Debug(msg, fields...)
}

func (log *Logger) Info(msg string, fields ...zap.Field) {
	log.z.Info(msg, fields...)
}

func (log *Logger) InfoWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, infoattribute, fields...)
	log.z.Info(msg, fields...)
}

func (log *Logger) Warn(msg string, fields ...zap.Field) {
	log.z.Warn(msg, fields...)
}

func (log *Logger) WarnWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, warnattribute, fields...)
	log.z.Warn(msg, fields...)
}

func (log *Logger) Error(msg string, fields ...zap.Field) {
	log.z.Error(msg, fields...)
}

func (log *Logger) ErrorWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, errorattribute, fields...)
	log.z.Error(msg, fields...)
}

func (log *Logger) DPanic(msg string, fields ...zap.Field) {
	log.z.DPanic(msg, fields...)
}

func (log *Logger) DPanicWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, dpanicattribute, fields...)
	log.z.DPanic(msg, fields...)
}

func (log *Logger) Panic(msg string, fields ...zap.Field) {
	log.z.Panic(msg, fields...)
}

func (log *Logger) PanicWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, panicattribute, fields...)
	log.z.Panic(msg, fields...)
}

func (log *Logger) Fatal(msg string, fields ...zap.Field) {
	log.z.Fatal(msg, fields...)
}

func (log *Logger) FatalWithTrace(ctx context.Context, msg string, fields ...zap.Field) {
	log.setAttributesToSpan(ctx, msg, fatalattribute, fields...)
	log.z.Fatal(msg, fields...)
}

func (log *Logger) setAttributesToSpan(ctx context.Context, msg string, lvl attribute.KeyValue, fields ...zap.Field) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		attrs := make([]attribute.KeyValue, 0, len(log.attrs)+len(fields)+2)
		// TODO: use Severity in OTEL
		attrs = append(attrs, lvl)
		if _, file, line, ok := runtime.Caller(attrCallDepth); ok {
			attrs = append(attrs, codeFilepathAttributeKey.String(file), codeLinenoAtrributeKey.Int(line))
			// attrs = append(attrs, attribute.String(callerAttributeKey, getCaller(file, line)))
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
