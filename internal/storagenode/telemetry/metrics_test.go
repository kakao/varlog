package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric/global"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestRegisterLogStreamMetrics(t *testing.T) {
	m, err := RegisterMetrics(global.Meter("test"), 1)
	assert.NoError(t, err)

	_, err = RegisterLogStreamMetrics(m, 1)
	assert.NoError(t, err)

	_, err = RegisterLogStreamMetrics(m, 1)
	assert.Error(t, err)

	UnregisterLogStreamMetrics(m, 1)

	_, err = RegisterLogStreamMetrics(m, 1)
	assert.NoError(t, err)
}
