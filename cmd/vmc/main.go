package main

import (
	"github.com/kakao/varlog/cmd/vmc/app"
)

func main() {
	if vmc, err := app.New(); err == nil {
		vmc.Execute()
	}
}
