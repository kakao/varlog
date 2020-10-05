package app

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/kakao/varlog/internal/vms"
	"go.uber.org/zap"
)

func Main(opts *vms.Options) error {
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}
	defer logger.Sync()

	opts.Logger = logger

	cm, err := vms.NewClusterManager(opts)
	if err != nil {
		logger.Error("could not create cluster manager server", zap.Error(err))
		return err
	}

	if err = cm.Run(); err != nil {
		logger.Error("could not run cluster manager server", zap.Error(err))
		return err
	}

	// TODO (jun): handle SIGQUIT (it should be able to produce core dump)
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-sigC:
			cm.Close()
		}
	}()

	cm.Wait()
	return nil
}
