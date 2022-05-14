package app

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/internal/metarepos"
	"github.daumkakao.com/varlog/varlog/pkg/util/log"
)

func Main(opts *metarepos.MetadataRepositoryOptions) error {
	path, err := filepath.Abs(opts.LogDir)
	if err != nil {
		fmt.Printf("could not create abs path:: %v\n", err)
		return err
	}

	logger, err := log.New(
		log.WithoutLogToStderr(),
		log.WithPath(fmt.Sprintf("%s/log.txt", path)),
	)
	if err != nil {
		fmt.Printf("could not create logger:: %v\n", err)
		return err
	}

	defer logger.Sync()

	opts.Logger = logger
	opts.ReporterClientFac = metarepos.NewReporterClientFactory(
		grpc.WithReadBufferSize(opts.ReportCommitterReadBufferSize),
		grpc.WithWriteBufferSize(opts.ReportCommitterWriteBufferSize),
	)

	mr := metarepos.NewRaftMetadataRepository(opts)
	mr.Run()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigC
		mr.Close()
	}()

	mr.Wait()
	return nil
}
