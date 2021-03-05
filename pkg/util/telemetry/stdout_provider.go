package telemetry

import (
	"context"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/exporters/stdout"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
)

type stdoutTelemetry struct {
	cont *controller.Controller
}

// var _ Telemetry = (*stdoutTelemetry)(nil)

func newStdoutPipeline(res *resource.Resource, logger *zap.Logger) (*stdoutTelemetry, error) {
	exportOpts := []stdout.Option{
		stdout.WithPrettyPrint(),
	}

	if logger != nil {
		stdLogger, err := zap.NewStdLogAt(logger, zap.InfoLevel)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		exportOpts = append(exportOpts, stdout.WithWriter(stdLogger.Writer()))
	}

	pipeline, err := stdout.InstallNewPipeline(exportOpts, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &stdoutTelemetry{cont: pipeline}, nil
}

func (sp *stdoutTelemetry) Close(ctx context.Context) error {
	return errors.WithStack(sp.cont.Stop(ctx))
}
