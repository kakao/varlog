package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/varlogadm"
)

func Main(opts *varlogadm.Options) error {
	// TODO: add VMSInitTimeout to options
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cm, err := varlogadm.NewClusterManager(ctx, opts)
	if err != nil {
		opts.Logger.Error("could not create cluster manager server", zap.Error(err))
		return err
	}

	if err = cm.Run(); err != nil {
		opts.Logger.Error("could not run cluster manager server", zap.Error(err))
		return err
	}

	// TODO (jun): handle SIGQUIT (it should be able to produce core dump)
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigC
		cm.Close()
	}()

	cm.Wait()
	return nil
}
