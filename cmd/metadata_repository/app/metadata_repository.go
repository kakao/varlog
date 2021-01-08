package app

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/pkg/util/log"
)

func Main(opts *metadata_repository.MetadataRepositoryOptions) error {
	path, err := filepath.Abs(opts.LogDir)
	if err != nil {
		fmt.Printf("could not create abs path:: %v\n", err)
		return err
	}

	lopts := log.Options{
		RotateOptions: log.RotateOptions{
			MaxSizeMB:  log.DefaultMaxSizeMB,
			MaxAgeDays: log.DefaultMaxAgeDay,
			MaxBackups: log.DefaultMaxBackups,
		},
		Path: fmt.Sprintf("%s/log.txt", path),
	}

	logger, err := log.NewInternal(lopts)
	if err != nil {
		fmt.Printf("could not create logger:: %v\n", err)
		return err
	}

	defer logger.Sync()

	opts.Logger = logger
	opts.ReporterClientFac = metadata_repository.NewReporterClientFactory()

	mr := metadata_repository.NewRaftMetadataRepository(opts)
	mr.Run()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-sigC:
			mr.Close()
		}
	}()

	mr.Wait()
	return nil
}
