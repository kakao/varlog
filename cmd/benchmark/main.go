package main

import (
	"fmt"
	"os"

	"github.daumkakao.com/varlog/varlog/cmd/benchmark/app"
)

func main() {
	app := app.New()
	if err := app.Run(os.Args); err != nil {
		fmt.Printf("%+v", err)
		os.Exit(1)
	}
}
