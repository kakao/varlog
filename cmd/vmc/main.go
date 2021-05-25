package main

import (
	"os"

	"github.daumkakao.com/varlog/varlog/cmd/vmc/app"
)

func main() {
	vmc, err := app.New()
	if err != nil {
		os.Exit(1)
	}

	if err := vmc.Execute(); err != nil {
		os.Exit(1)
	}
}
