package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

func ExampleLogger() {
	logger, err := New()
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	logger.Info("this is log", zap.String("example", "first"))
}

func TestEnableDevelopmentMode(t *testing.T) {
	tcs := []struct {
		name       string
		zapOpts    []zap.Option
		assertFunc func(t require.TestingT, f assert.PanicTestFunc, msgAndArgs ...any)
	}{
		{
			name:       "DevelopmentMode",
			zapOpts:    []zap.Option{zap.Development()},
			assertFunc: require.Panics,
		},
		{
			name:       "ProductionMode",
			zapOpts:    nil,
			assertFunc: require.NotPanics,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			logger, err := New(WithZapLoggerOptions(tc.zapOpts...))
			require.NoError(t, err)
			defer func() {
				_ = logger.Sync()
			}()

			tc.assertFunc(t, func() {
				logger.DPanic("panic occur")
			})
		})
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
