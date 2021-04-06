package main

import (
	"os"

	"github.com/kakao/varlog/cmd/storagenode/app"
	"github.com/kakao/varlog/cmd/storagenode/config"
)

func main() {
	app := app.InitCLI(&config.Config{})
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
