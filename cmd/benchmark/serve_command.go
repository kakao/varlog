package main

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/benchmark/server"
)

var (
	flagAddr = &cli.StringFlag{
		Name:  "addr",
		Value: server.DefaultAddress,
	}
	flagDatabaseHost = &cli.StringFlag{
		Name:     "database-host",
		Value:    server.DefaultDatabaseHost,
		Category: "database",
		Usage:    "Database host",
	}
	flagDatabasePort = &cli.IntFlag{
		Name:     "database-port",
		Value:    server.DefaultDatabasePort,
		Category: "database",
		Usage:    "Database port",
	}
	flagDatabaseUser = &cli.StringFlag{
		Name:     "database-user",
		Value:    server.DefaultDatabaseUser,
		Category: "database",
		Usage:    "Database user",
	}
	flagDatabasePassword = &cli.StringFlag{
		Name:     "database-password",
		Category: "database",
		Usage:    "Database password",
	}
	flagDatabaseName = &cli.StringFlag{
		Name:     "database-name",
		Value:    server.DefaultDatabaseName,
		Category: "database",
		Usage:    "Database name",
	}
)

func newCommandServe() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "run benchmark web application",
		Flags: []cli.Flag{
			flagAddr,
			flagDatabaseHost,
			flagDatabasePort,
			flagDatabaseUser,
			flagDatabasePassword,
			flagDatabaseName,
		},
		Action: runCommandServe,
	}
}

func runCommandServe(c *cli.Context) error {
	server, err := server.New(
		server.WithAddress(c.String(flagAddr.Name)),
		server.WithDatabaseHost(c.String(flagDatabaseHost.Name)),
		server.WithDatabasePort(c.Int(flagDatabasePort.Name)),
		server.WithDatabaseUser(c.String(flagDatabaseUser.Name)),
		server.WithDatabasePassword(c.String(flagDatabasePassword.Name)),
		server.WithDatabaseName(c.String(flagDatabaseName.Name)),
	)
	if err != nil {
		slog.Error("could not create server", slog.Any("error", err))
		os.Exit(1)
	}

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	stopper := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		select {
		case sig := <-sigC:
			slog.Info("caught signal", slog.String("signal", sig.String()))
			_ = server.Close()
		case <-stopper:
		}
	}()
	go func() {
		defer func() {
			close(stopper)
			wg.Done()
		}()
		err = server.Run()
	}()
	wg.Wait()
	if err != nil {
		slog.Error("server stopped abnormally", slog.Any("error", err))
		os.Exit(1)
	}
	return nil
}
