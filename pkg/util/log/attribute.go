package log

import (
	"math"
	"time"

	"go.opentelemetry.io/otel/label"
	"go.uber.org/zap/zapcore"
)

func fieldToAttribute(f zapcore.Field) label.KeyValue {
	switch f.Type {
	case zapcore.StringType:
		return label.String(f.Key, f.String)

	case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type:
		return label.Int32(f.Key, int32(f.Integer))
	case zapcore.Int64Type:
		return label.Int64(f.Key, f.Integer)

	case zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type:
		return label.Uint32(f.Key, uint32(f.Integer))
	case zapcore.Uint64Type:
		return label.Uint64(f.Key, uint64(f.Integer))

	case zapcore.Float32Type:
		return label.Float32(f.Key, math.Float32frombits(uint32(f.Integer)))

	case zapcore.Float64Type:
		return label.Float64(f.Key, math.Float64frombits(uint64(f.Integer)))

	case zapcore.BoolType:
		return label.Bool(f.Key, f.Integer == 1)

	case zapcore.DurationType:
		return label.String(f.Key, time.Duration(f.Integer).String())

	case zapcore.ErrorType:
		return label.String(f.Key, f.Interface.(error).Error())

	case zapcore.ArrayMarshalerType:
		return label.Array(f.Key, f.Interface)

	case zapcore.BinaryType, zapcore.ByteStringType:
		return label.String(f.Key, string(f.Interface.([]byte)))

	default:
		return label.Any(f.Key, f.Interface)
	}
}

func fieldsToAttributes(fields ...zapcore.Field) []label.KeyValue {
	attrs := make([]label.KeyValue, 0, len(fields))
	return appendAttributesByFields(attrs, fields...)
}

func appendAttributesByFields(attrs []label.KeyValue, fields ...zapcore.Field) []label.KeyValue {
	for i := range fields {
		attrs = append(attrs, fieldToAttribute(fields[i]))
	}
	return attrs
}
