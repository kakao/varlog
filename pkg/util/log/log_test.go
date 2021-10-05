package log

import (
	"go.uber.org/zap"
)

func ExampleLogger() {
	logger, err := New()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	logger.Info("this is log", zap.String("example", "first"))
}
