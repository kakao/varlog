package app

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/kakao/varlog/cmd/storagenode/config"
	"github.com/kakao/varlog/internal/storagenode"
)

func Main(cfg *config.Config) error {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Sampling = nil
	loggerConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	loggerConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := loggerConfig.Build(zap.AddStacktrace(zap.DPanicLevel))
	if err != nil {
		return err
	}
	defer logger.Sync()

	// TODO: add initTimeout option
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sn, err := storagenode.New(ctx,
		storagenode.WithClusterID(cfg.ClusterID),
		storagenode.WithStorageNodeID(cfg.StorageNodeID),
		storagenode.WithListenAddress(cfg.ListenAddress),
		storagenode.WithAdvertiseAddress(cfg.AdvertiseAddress),
		storagenode.WithVolumes(cfg.Volumes...),
	)
	if err != nil {
		log.Fatalf("could not create StorageNode: %v", err)
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

	return sn.Run()
}
