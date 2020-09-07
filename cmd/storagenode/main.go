package main

import (
	"os"

	"github.daumkakao.com/varlog/varlog/cmd/storagenode/app"
	"github.daumkakao.com/varlog/varlog/internal/storage"
)

func main() {
	options := &storage.StorageNodeOptions{}
	app := app.InitCLI(options)
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
