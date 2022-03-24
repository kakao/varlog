package main

import (
	"log"
	"os"

	_ "go.uber.org/automaxprocs"

	"github.daumkakao.com/varlog/varlog/cmd/varlogadm/app"
)

func main() {
	app := app.InitCLI()
	if err := app.Run(os.Args); err != nil {
		log.Printf("varlogadm: %v", err)
		os.Exit(1)
	}
}
