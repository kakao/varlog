package telemetry

import (
	"context"
	"time"

	metricsdk "go.opentelemetry.io/otel/sdk/export/metric"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"

	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
)

const (
	defaultReconnectPeriod = 10 * time.Second
)

type exporter struct {
	traceExporter  tracesdk.SpanExporter
	metricExporter metricsdk.Exporter
}

type exporterConfig struct {
	// OLTP exporter
	otlpEndpoint   string
	otlpCompressor string

	// Stdout exporter
	stdoutLogger *zap.Logger
	prettyPrint  bool
}

func newExporter(ctx context.Context, exporterType string, cfg exporterConfig) (*exporter, error) {
	switch exporterType {
	case "stdout", "console":
		return newStdoutExporter(cfg.stdoutLogger, cfg.prettyPrint)
	case "otlp", "otel":
		return newOTLPExporter(ctx, cfg.otlpEndpoint)
	default:
		return nil, errors.Errorf("unknown exporter: %s", exporterType)
	}
}

func newOTLPExporter(ctx context.Context, endpoint string) (*exporter, error) {
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithReconnectionPeriod(defaultReconnectPeriod),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithDialOption(grpc.WithBlock()),
		otlpmetricgrpc.WithReconnectionPeriod(defaultReconnectPeriod),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &exporter{traceExporter: traceExporter, metricExporter: metricExporter}, nil
}

func newStdoutExporter(logger *zap.Logger, pretty bool) (*exporter, error) {
	var opts []stdouttrace.Option
	if pretty {
		opts = append(opts, stdouttrace.WithPrettyPrint())
	}
	if logger != nil {
		stdLogger, err := zap.NewStdLogAt(logger, zap.InfoLevel)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		opts = append(opts, stdouttrace.WithWriter(stdLogger.Writer()))
	}

	traceExporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, err
	}

	metricExporter, err := stdoutmetric.New(
		stdoutmetric.WithPrettyPrint(),
	)
	if err != nil {
		return nil, err
	}

	return &exporter{traceExporter: traceExporter, metricExporter: metricExporter}, nil
}
