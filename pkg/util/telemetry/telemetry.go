package telemetry

import "context"

type Telemetry struct {
	provider Provider
}

type Provider interface {
	Close(ctx context.Context) error
}

func New(providerName string, collectorEndpoint string) (*Telemetry, error) {
	provider, err := newProvider(providerName, collectorEndpoint)
	if err != nil {
		return nil, err
	}
	tm := &Telemetry{
		provider: provider,
	}
	return tm, nil
}

func newProvider(name string, collectorEndpoint string) (Provider, error) {
	switch name {
	case "stdout":
		return newStdoutProvider()
	case "simple":
		return newSimpleProvider("varlog", nil, collectorEndpoint)
	case "otel":
		return newOTELProvider(context.TODO(), "varlog", collectorEndpoint)
	default:
		return newNopProvider(), nil
	}
}

func (t *Telemetry) Close(ctx context.Context) {
	if t.provider != nil {
		t.provider.Close(ctx)
	}
}
