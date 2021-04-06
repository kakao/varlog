package main

import (
	"os"

	"github.daumkakao.com/varlog/varlog/cmd/storagenode/app"
	"github.daumkakao.com/varlog/varlog/cmd/storagenode/config"
)

func main() {
	app := app.InitCLI(&config.Config{})
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
