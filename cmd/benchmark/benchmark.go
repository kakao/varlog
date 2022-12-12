package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	os.Exit(run())
}

func run() int {
	app := newApp()
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		return -1
	}
	return 0
}

func newApp() *cli.App {
	app := &cli.App{
		Name: "benchmark",
		Commands: []*cli.Command{
			newCommandTest(),
			newCommandServe(),
			newCommandInitdb(),
		},
	}
	return app
}
