package trace

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/label"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/exporter"
)

const (
	StorageNodeLabelKeyPrefix        = "varlog.storagenode"
	LogStreamLabelKeyPrefix          = "varlog.storagenode.logstream"
	MetadataRepositoryLabelKeyPrefix = "varlog.metadatarepository"
)

var (
	StorageNodeIDLabelKey = label.Key("varlog.storagenode.id")
	LogStreamIDLabelKey   = label.Key("varlog.storagenode.logstream.id")
)

func StorageNodeIDLabel(v types.StorageNodeID) label.KeyValue {
	return label.KeyValue{
		Key:   StorageNodeIDLabelKey,
		Value: label.Uint32Value(uint32(v)),
	}
}

func LogStreamIDLabel(v types.LogStreamID) label.KeyValue {
	return label.KeyValue{
		Key:   LogStreamIDLabelKey,
		Value: label.Uint32Value(uint32(v)),
	}
}

type Tracer = oteltrace.Tracer
type Span = oteltrace.Span
type SpanOption = oteltrace.SpanOption

type Closer func(ctx context.Context) error

func New(exporter exporter.Exporter) Closer {
	bsp := sdktrace.NewBatchSpanProcessor(exporter)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(bsp))
	otel.SetTracerProvider(tp)
	closer := func(ctx context.Context) error {
		return tp.Shutdown(ctx)
	}
	return closer
}

func NamedTracer(name string) Tracer {
	return otel.Tracer(name)
}
