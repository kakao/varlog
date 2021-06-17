package main

import (
	"os"

	"github.com/kakao/varlog/cmd/storagenode/app"
)

func main() {
	app := app.InitCLI()
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
