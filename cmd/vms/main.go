package main

import (
	"log"
	"os"

	"github.daumkakao.com/varlog/varlog/cmd/vms/app"
	"github.daumkakao.com/varlog/varlog/internal/vms"
)

func main() {
	options := vms.DefaultOptions()
	app := app.InitCLI(&options)
	if err := app.Run(os.Args); err != nil {
		log.Printf("vms: %v", err)
		os.Exit(1)
	}
}
