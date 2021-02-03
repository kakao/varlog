package app

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storagenode"
)

func Main(opts *storagenode.Options) error {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Sampling = nil
	loggerConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logger, err := loggerConfig.Build(zap.AddStacktrace(zap.DPanicLevel))
	if err != nil {
		return err
	}
	defer logger.Sync()

	opts.Logger = logger

	sn, err := storagenode.NewStorageNode(opts)
	if err != nil {
		log.Fatalf("could not create StorageNode: %v", err)
		return err
	}
	if err = sn.Run(); err != nil {
		log.Fatalf("could not run StorageNode: %v", err)
		return err
	}

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-sigC:
			sn.Close()
		}
	}()

	sn.Wait()
	// TODO (jun): it should be the reason why storagenode process is stopped
	return nil
}
