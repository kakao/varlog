package log

import (
	"testing"

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

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
