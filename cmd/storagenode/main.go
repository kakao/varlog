package main

import (
	"os"

	"github.com/kakao/varlog/cmd/storagenode/app"
	"github.com/kakao/varlog/internal/storagenode"
)

func main() {
	options := &storagenode.StorageNodeOptions{}
	app := app.InitCLI(options)
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
