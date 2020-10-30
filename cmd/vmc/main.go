package main

import (
	"github.daumkakao.com/varlog/varlog/cmd/vmc/app"
)

func main() {
	if vmc, err := app.New(); err == nil {
		vmc.Execute()
	}
}
