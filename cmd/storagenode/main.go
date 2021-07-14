package main

import (
	"os"

	_ "go.uber.org/automaxprocs"

	"github.com/kakao/varlog/cmd/storagenode/app"
)

func main() {
	app := app.InitCLI()
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
