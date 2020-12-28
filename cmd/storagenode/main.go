package main

import (
	"os"

	"github.daumkakao.com/varlog/varlog/cmd/storagenode/app"
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
)

func main() {
	options := storagenode.DefaultOptions()
	app := app.InitCLI(&options)
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
