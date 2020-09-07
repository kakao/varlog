package app

import (
	"log"

	"github.com/kakao/varlog/internal/storage"
	"go.uber.org/zap"
)

func Main(opts *storage.StorageNodeOptions) error {
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}
	defer logger.Sync()

	opts.Logger = logger

	sn, err := storage.NewStorageNode(opts)
	if err != nil {
		log.Fatalf("could not create StorageNode: %v", err)
		return err
	}
	if err = sn.Run(); err != nil {
		log.Fatalf("could not run StorageNode: %v", err)
		return err
	}
	sn.Wait()
	// TODO (jun): it should be the reason why storagenode process is stopped
	return nil
}
