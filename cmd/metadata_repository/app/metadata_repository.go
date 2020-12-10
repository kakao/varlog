package app

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/pkg/util/loggerutil"
)

func Main(opts *metadata_repository.MetadataRepositoryOptions) error {
	path, err := filepath.Abs(opts.LogDir)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(path, 0750); err != nil {
		return err
	}

	lopts := loggerutil.Options{
		RotateOptions: loggerutil.RotateOptions{
			MaxSizeMB:  loggerutil.DefaultMaxSizeMB,
			MaxAgeDays: loggerutil.DefaultMaxAgeDay,
			MaxBackups: loggerutil.DefaultMaxBackups,
		},
		Path: fmt.Sprintf("%s/log.txt", path),
	}

	logger, err := loggerutil.New(lopts)
	if err != nil {
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
