package main

import (
	"log"
	"os"

	_ "go.uber.org/automaxprocs"

	"github.com/kakao/varlog/cmd/varlogadm/app"
	"github.com/kakao/varlog/internal/varlogadm"
)

func main() {
	options := varlogadm.DefaultOptions()
	app := app.InitCLI(&options)
	if err := app.Run(os.Args); err != nil {
		log.Printf("varlogadm: %v", err)
		os.Exit(1)
	}
}
