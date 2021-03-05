package telemetry

import (
	"context"
	"time"

	"github.com/pkg/errors"
	otlpexporter "go.opentelemetry.io/otel/exporters/otlp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpgrpc"
	"go.opentelemetry.io/otel/exporters/stdout"
	stdoutexporter "go.opentelemetry.io/otel/exporters/stdout"
	"go.uber.org/zap"
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/kakao/varlog/pkg/util/telemetry/metric"
	"github.com/kakao/varlog/pkg/util/telemetry/trace"
)

const (
	defaultReconnectPeriod = 10 * time.Second
)

type exporter interface {
	trace.SpanExporter
	metric.Exporter
}

type exporterConfig struct {
	// OLTP exporter
	otlpEndpoint   string
	otlpCompressor string

	// Stdout exporter
	stdoutLogger *zap.Logger
	prettyPrint  bool
}

func newExporter(ctx context.Context, exporterType string, cfg exporterConfig) (exporter, error) {
	switch exporterType {
	case "stdout", "console":
		return newStdoutExporter(cfg.stdoutLogger, cfg.prettyPrint, false, false)
	case "otlp", "otel":
		return newOTLPExporter(ctx, cfg.otlpEndpoint, cfg.otlpCompressor)
	case "nop":
		return newNopExporter()
	default:
		return nil, errors.Errorf("unknown exporter: %s", exporterType)
	}
}

func newOTLPExporter(ctx context.Context, endpoint string, compressor string) (exporter, error) {
	driverOpts := []otlpgrpc.Option{
		otlpgrpc.WithInsecure(),
		otlpgrpc.WithEndpoint(endpoint),
		otlpgrpc.WithReconnectionPeriod(defaultReconnectPeriod),
	}
	if compressor == "gzip" {
		driverOpts = append(driverOpts, otlpgrpc.WithCompressor(compressor))
	}
	driver := otlpgrpc.NewDriver(driverOpts...)
	exp, err := otlpexporter.NewExporter(ctx, driver)
	return exp, errors.WithStack(err)
}

func newStdoutExporter(logger *zap.Logger, pretty, disableTrace, disableMetric bool) (exporter, error) {
	var opts []stdoutexporter.Option
	if pretty {
		opts = append(opts, stdoutexporter.WithPrettyPrint())
	}
	if disableTrace {
		opts = append(opts, stdoutexporter.WithoutTraceExport())
	}
	if disableMetric {
		opts = append(opts, stdoutexporter.WithoutMetricExport())
	}
	if logger != nil {
		stdLogger, err := zap.NewStdLogAt(logger, zap.InfoLevel)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		opts = append(opts, stdout.WithWriter(stdLogger.Writer()))
	}
	return stdoutexporter.NewExporter(opts...)
}

func newNopExporter() (exporter, error) {
	return newStdoutExporter(nil, false, true, true)
}
