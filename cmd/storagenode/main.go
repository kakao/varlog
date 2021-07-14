package main

import (
	"os"

	_ "go.uber.org/automaxprocs"

	"github.daumkakao.com/varlog/varlog/cmd/storagenode/app"
)

func main() {
	app := app.InitCLI()
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
