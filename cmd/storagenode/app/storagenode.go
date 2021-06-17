package app

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/executor"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func Main(c *cli.Context) error {
	cid, err := types.ParseClusterID(c.String(flagClusterID.Name))
	if err != nil {
		return err
	}
	snid, err := types.ParseStorageNodeID(c.String(flagStorageNodeID.Name))
	if err != nil {
		return err
	}

	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Sampling = nil
	loggerConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	loggerConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, err := loggerConfig.Build(zap.AddStacktrace(zap.DPanicLevel))
	if err != nil {
		return err
	}
	defer logger.Sync()

	var storageOpts []storage.Option
	if c.Bool(flagDisableWriteSync.Name) {
		storageOpts = append(storageOpts, storage.WithoutWriteSync())
	}
	if c.Bool(flagDisableCommitSync.Name) {
		storageOpts = append(storageOpts, storage.WithoutCommitSync())
	}
	if c.Bool(flagDisableDeleteCommittedSync.Name) {
		storageOpts = append(storageOpts, storage.WithoutDeleteCommittedSync())
	}
	if c.Bool(flagDisableDeleteUncommittedSync.Name) {
		storageOpts = append(storageOpts, storage.WithoutDeleteUncommittedSync())
	}

	// TODO: add initTimeout option
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sn, err := storagenode.New(ctx,
		storagenode.WithClusterID(cid),
		storagenode.WithStorageNodeID(snid),
		storagenode.WithListenAddress(c.String(flagListenAddress.Name)),
		storagenode.WithAdvertiseAddress(c.String(flagAdvertiseAddress.Name)),
		storagenode.WithVolumes(c.StringSlice(flagVolumes.Name)...),
		storagenode.WithExecutorOptions(
			executor.WithWriteQueueSize(c.Int(flagWriteQueueSize.Name)),
			executor.WithWriteBatchSize(c.Int(flagWriteBatchSize.Name)),
			executor.WithCommitQueueSize(c.Int(flagCommitQueueSize.Name)),
			executor.WithCommitBatchSize(c.Int(flagCommitBatchSize.Name)),
			executor.WithReplicateQueueSize(c.Int(flagReplicateQueueSize.Name)),
		),
		storagenode.WithStorageOptions(storageOpts...),
		storagenode.WithLogger(logger),
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
