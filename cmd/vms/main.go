package main

import (
	"log"
	"os"

	_ "go.uber.org/automaxprocs"

	"github.com/kakao/varlog/cmd/vms/app"
	"github.com/kakao/varlog/internal/vms"
)

func main() {
	options := vms.DefaultOptions()
	app := app.InitCLI(&options)
	if err := app.Run(os.Args); err != nil {
		log.Printf("vms: %v", err)
		os.Exit(1)
	}
}
