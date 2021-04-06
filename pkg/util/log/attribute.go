package log

import (
	"math"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap/zapcore"
)

func fieldToAttribute(f zapcore.Field) attribute.KeyValue {
	switch f.Type {
	case zapcore.StringType:
		return attribute.String(f.Key, f.String)

	case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type:
		return attribute.Int64(f.Key, f.Integer)
	case zapcore.Int64Type:
		return attribute.Int64(f.Key, f.Integer)

	case zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type:
		return attribute.Int64(f.Key, int64(f.Integer))
	case zapcore.Uint64Type:
		// FIXME: handle unsigned integer
		return attribute.Int64(f.Key, int64(f.Integer))

	case zapcore.Float32Type:
		return attribute.Float64(f.Key, math.Float64frombits(uint64(f.Integer)))

	case zapcore.Float64Type:
		return attribute.Float64(f.Key, math.Float64frombits(uint64(f.Integer)))

	case zapcore.BoolType:
		return attribute.Bool(f.Key, f.Integer == 1)

	case zapcore.DurationType:
		return attribute.String(f.Key, time.Duration(f.Integer).String())

	case zapcore.ErrorType:
		return attribute.String(f.Key, f.Interface.(error).Error())

	case zapcore.ArrayMarshalerType:
		return attribute.Array(f.Key, f.Interface)

	case zapcore.BinaryType, zapcore.ByteStringType:
		return attribute.String(f.Key, string(f.Interface.([]byte)))

	default:
		return attribute.Any(f.Key, f.Interface)
	}
}

func fieldsToAttributes(fields ...zapcore.Field) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(fields))
	return appendAttributesByFields(attrs, fields...)
}

func appendAttributesByFields(attrs []attribute.KeyValue, fields ...zapcore.Field) []attribute.KeyValue {
	for i := range fields {
		attrs = append(attrs, fieldToAttribute(fields[i]))
	}
	return attrs
}
