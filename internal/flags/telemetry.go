package flags

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.32.0"

	"github.com/kakao/varlog/internal/stats/opentelemetry"
	"github.com/kakao/varlog/pkg/types"
)

const (
	CategoryTelemetry = "Telemetry:"

	TelemetryExporterNOOP   = "noop"
	TelemetryExporterStdout = "stdout"
	TelemetryExporterOTLP   = "otlp"

	DefaultTelemetryOTLPEndpoint = "localhost:4317"

	DefaultTelemetryStopTimeout = 3 * time.Second
)

var (
	TelemetryExporter = &cli.StringFlag{
		Name:     "telemetry-exporter",
		Category: CategoryTelemetry,
		Aliases:  []string{"exporter-type"},
		Usage:    fmt.Sprintf("Exporter type: %s, %s or %s.", TelemetryExporterNOOP, TelemetryExporterStdout, TelemetryExporterOTLP),
		EnvVars:  []string{"TELEMETRY_EXPORTER", "EXPORTER_TYPE"},
		Value:    TelemetryExporterNOOP,
		Action: func(_ *cli.Context, value string) error {
			switch strings.ToLower(value) {
			case TelemetryExporterNOOP, TelemetryExporterStdout, TelemetryExporterOTLP:
				return nil
			default:
				return fmt.Errorf("invalid value \"%s\" for flag --telemetry-exporter", value)
			}
		},
	}
	TelemetryOTLPEndpoint = &cli.StringFlag{
		Name:     "telemetry-otlp-endpoint",
		Category: CategoryTelemetry,
		Aliases:  []string{"exporter-otlp-endpoint"},
		Usage:    "Endpoint for OTLP exporter.",
		EnvVars:  []string{"TELEMETRY_OTLP_ENDPOINT", "EXPORTER_OTLP_ENDPOINT"},
		Value:    DefaultTelemetryOTLPEndpoint,
		Action: func(c *cli.Context, value string) error {
			if c.String(TelemetryExporter.Name) != TelemetryExporterOTLP || value != "" {
				return nil
			}
			return errors.New("no value for flag --telemetry-otlp-endpoint")
		},
	}
	TelemetryOTLPInsecure = &cli.BoolFlag{
		Name:     "telemetry-otlp-insecure",
		Category: CategoryTelemetry,
		Aliases:  []string{"exporter-otlp-insecure"},
		Usage:    "Disable gRPC client transport security for OTLP exporter.",
		EnvVars:  []string{"TELEMETRY_OTLP_INSECURE", "EXPORTER_OTLP_INSECURE"},
	}
	TelemetryExporterStopTimeout = &cli.DurationFlag{
		Name:     "telemetry-exporter-stop-timeout",
		Category: CategoryTelemetry,
		Aliases:  []string{"expoter-stop-timeout"},
		Usage:    "Timeout for stopping OTLP exporter.",
		EnvVars:  []string{"TELEMETRY_EXPORTER_STOP_TIMEOUT", "EXPORTER_STOP_TIMEOUT"},
		Value:    DefaultTelemetryStopTimeout,
	}
	TelemetryHost = &cli.BoolFlag{
		Name:     "telemetry-host",
		Category: CategoryTelemetry,
		Usage:    "Export host metrics.",
		EnvVars:  []string{"TELEMETRY_HOST"},
	}
	TelemetryRuntime = &cli.BoolFlag{
		Name:     "telemetry-runtime",
		Category: CategoryTelemetry,
		Usage:    "Export runtime metrics.",
		EnvVars:  []string{"TELEMETRY_RUNTIME"},
	}
)

func ParseTelemetryFlags(ctx context.Context, c *cli.Context, serviceName, serviceInstanceID string, cid types.ClusterID) (opts []opentelemetry.MeterProviderOption, err error) {
	const serviceNamespace = "varlog"

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithHost(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceNamespace(serviceNamespace),
			semconv.ServiceInstanceID(serviceInstanceID),
			opentelemetry.ClusterID(cid),
		))
	if err != nil {
		return nil, err
	}
	opts = append(opts, opentelemetry.WithResource(res))

	var exporter metricsdk.Exporter

	switch strings.ToLower(c.String(TelemetryExporter.Name)) {
	case TelemetryExporterStdout:
		exporter, err = opentelemetry.NewStdoutExporter()
	case TelemetryExporterOTLP:
		var opts []otlpmetricgrpc.Option
		if c.Bool(TelemetryOTLPInsecure.Name) {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		opts = append(opts, otlpmetricgrpc.WithEndpoint(c.String(TelemetryOTLPEndpoint.Name)))
		exporter, err = opentelemetry.NewOLTPExporter(context.Background(), opts...)
	case TelemetryExporterNOOP:
		exporter = nil
	}
	if err != nil {
		return nil, err
	}

	opts = append(opts, opentelemetry.WithExporter(exporter))

	if c.Bool(TelemetryHost.Name) {
		opts = append(opts, opentelemetry.WithHostInstrumentation())
	}
	if c.Bool(TelemetryRuntime.Name) {
		opts = append(opts, opentelemetry.WithRuntimeInstrumentation())
	}

	return opts, nil
}
