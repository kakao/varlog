package main

import (
	"os"

	"github.daumkakao.com/varlog/varlog/cmd/metadata_repository/app"
	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
)

func main() {
	options := &metadata_repository.MetadataRepositoryOptions{}
	app := app.InitCLI(options)
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
