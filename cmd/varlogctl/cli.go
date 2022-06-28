package main

import (
	"time"

	"github.com/urfave/cli/v2"
)

const (
	appName = "varlogctl"
	version = "0.0.1"

	defaultTimeout = time.Second * 5
)

func commonFlags(flags ...cli.Flag) []cli.Flag {
	return append([]cli.Flag{
		flagAdminAddress.StringFlag(true, ""),
		flagTimeout.DurationFlag(false, defaultTimeout),
		flagPrettyPrint.BoolFlag(),
		flagVerbose.BoolFlag(),
	}, flags...)
}

func newVarlogControllerApp() *cli.App {
	app := &cli.App{
		Name:    appName,
		Usage:   "controller application for varlog",
		Version: version,
		Commands: []*cli.Command{
			newStorageNodeCommand(),
			newTopicCommand(),
			newLogStreamCommand(),
			newMetadataRepositoryCommand(),
		},
	}
	return app
}
