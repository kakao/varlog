package main

import (
	"os"

	_ "go.uber.org/automaxprocs"

	"github.daumkakao.com/varlog/varlog/cmd/varlogmr/app"
	"github.daumkakao.com/varlog/varlog/internal/metarepos"
)

func main() {
	options := &metarepos.MetadataRepositoryOptions{}
	app := app.InitCLI(options)
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
