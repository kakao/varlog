package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/pkg/types"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestRegisterLogStreamMetrics(t *testing.T) {
	const tpid = types.TopicID(1)
	const lsid = types.LogStreamID(2)

	m, err := RegisterMetrics(otel.Meter("test"))
	assert.NoError(t, err)

	_, err = RegisterLogStreamMetrics(m, tpid, lsid)
	assert.NoError(t, err)

	_, err = RegisterLogStreamMetrics(m, tpid, lsid)
	assert.Error(t, err)

	UnregisterLogStreamMetrics(m, lsid)

	_, err = RegisterLogStreamMetrics(m, tpid, lsid)
	assert.NoError(t, err)
}
