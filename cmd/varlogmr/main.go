package main

import (
	"os"

	_ "go.uber.org/automaxprocs"

	"github.com/kakao/varlog/cmd/varlogmr/app"
	"github.com/kakao/varlog/internal/metarepos"
)

func main() {
	options := &metarepos.MetadataRepositoryOptions{}
	app := app.InitCLI(options)
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
