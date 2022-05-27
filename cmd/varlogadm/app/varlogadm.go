package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/internal/admin"
)

func Main(opts []admin.Option, logger *zap.Logger) error {
	// TODO: add VMSInitTimeout to options
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cm, err := admin.New(ctx, opts...)
	if err != nil {
		logger.Error("could not create cluster manager server", zap.Error(err))
		return err
	}

	var g errgroup.Group
	quit := make(chan struct{})
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)

	g.Go(func() error {
		defer close(quit)
		return cm.Serve()
	})
	g.Go(func() error {
		select {
		case sig := <-sigC:
			return multierr.Append(fmt.Errorf("caught signal %s", sig), cm.Close())
		case <-quit:
			return nil
		}
	})
	return g.Wait()
}
