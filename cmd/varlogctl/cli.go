package main

import (
	"fmt"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/buildinfo"
)

const (
	appName = "varlogctl"

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
	buildInfo := buildinfo.ReadVersionInfo()
	cli.VersionPrinter = func(*cli.Context) {
		fmt.Println(buildInfo.String())
	}
	app := &cli.App{
		Name:    appName,
		Usage:   "controller application for varlog",
		Version: buildInfo.Version,
		Commands: []*cli.Command{
			newStorageNodeCommand(),
			newTopicCommand(),
			newLogStreamCommand(),
			newMetadataRepositoryCommand(),
		},
	}
	return app
}
