package app

import (
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/metadata_repository"
)

func Main(opts *metadata_repository.MetadataRepositoryOptions) error {
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}
	defer logger.Sync()

	opts.Logger = logger
	opts.ReporterClientFac = metadata_repository.NewReporterClientFactory()

	mr := metadata_repository.NewRaftMetadataRepository(opts)
	mr.Run()
	mr.Wait()
	// TODO:: it should be the reason why storagenode process is stopped
	return nil
}
