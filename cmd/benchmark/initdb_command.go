package main

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/benchmark/database"
	"github.com/kakao/varlog/internal/benchmark/initdb"
)

var (
	flagReset = &cli.BoolFlag{
		Name:  "reset",
		Usage: "Drop tables if it is set",
		Value: false,
	}
)

func newCommandInitdb() *cli.Command {
	return &cli.Command{
		Name:  "initdb",
		Usage: "Initialize benchmark database",
		Flags: []cli.Flag{
			flagDatabaseHost,
			flagDatabasePort,
			flagDatabaseUser,
			flagDatabasePassword,
			flagDatabaseName,
			flagReset,
		},
		Action: runCommandInitdb,
	}
}

func runCommandInitdb(c *cli.Context) error {
	db, err := database.ConnectDatabase(
		c.String(flagDatabaseHost.Name),
		c.Int(flagDatabasePort.Name),
		c.String(flagDatabaseUser.Name),
		c.String(flagDatabasePassword.Name),
		c.String(flagDatabaseName.Name),
	)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()

	ctx := context.Background()
	if c.Bool(flagReset.Name) {
		if err := initdb.DropTables(ctx, db); err != nil {
			return fmt.Errorf("drop tables: %w", err)
		}
	}
	if err := initdb.CreateTables(ctx, db); err != nil {
		return fmt.Errorf("create tables: %w", err)
	}
	if err := initdb.InitTables(ctx, db); err != nil {
		return fmt.Errorf("insert rows: %w", err)
	}
	return nil
}
