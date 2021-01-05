package exporter

import (
	"log"

	"go.opentelemetry.io/otel/exporters/stdout"
)

func NewStdoutExporter() (Exporter, error) {
	exporter, err := stdout.NewExporter()
	if err != nil {
		log.Fatalf("failed to initialize stdout exporter pipeline: %v", err)
		return nil, err
	}
	return exporter, nil
}
