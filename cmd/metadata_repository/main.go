package main

import (
	"os"

	_ "go.uber.org/automaxprocs"

	"github.com/kakao/varlog/cmd/metadata_repository/app"
	"github.com/kakao/varlog/internal/metadata_repository"
)

func main() {
	options := &metadata_repository.MetadataRepositoryOptions{}
	app := app.InitCLI(options)
	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}
