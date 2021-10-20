package app

import (
	"context"
	"fmt"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/admin"
)

type VMCApp struct {
	app     *cli.App
	options Options
	logger  *zap.Logger
}

func New() (*VMCApp, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	zap.ReplaceGlobals(logger)
	defer logger.Sync()

	app := &VMCApp{
		logger: logger,
	}
	app.initCLI()
	return app, nil
}

func (app *VMCApp) Execute() error {
	return app.app.Run(os.Args)
}

type CommandExecutor func(ctx context.Context, cli admin.Client) (proto.Message, error)

func (app *VMCApp) withExecutionContext(f CommandExecutor) {
	if !app.options.Verbose {
		app.logger = zap.NewNop()
	}

	if err := app.execute(f); err != nil {
		fmt.Fprintf(os.Stderr, "vmc: %v", err)
		os.Exit(1)
	}
}

func (app *VMCApp) execute(f CommandExecutor) error {
	ctx, cancel := context.WithTimeout(context.Background(), app.options.Timeout)
	defer cancel()

	cli, err := admin.New(ctx, app.options.VMSAddress)
	if err != nil {
		app.logger.Error("could not create client", zap.Error(err))
		return err
	}
	defer func() {
		if err := cli.Close(); err != nil {
			zap.L().Error("error while closing client", zap.Error(err))
		}
	}()
	app.logger.Debug("connected to VMS", zap.String("vms", app.options.VMSAddress))

	msg, err := f(ctx, cli)
	if err != nil {
		app.logger.Error("error", zap.Error(err))
		return err
	}
	printer := &jsonPrinter{}
	if err := printer.Print(os.Stdout, msg); err != nil {
		zap.L().Error("print error", zap.Error(err))
	}
	return nil
}
