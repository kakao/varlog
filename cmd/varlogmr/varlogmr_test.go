package main

import (
	"flag"
	"os"
	"testing"

	"github.com/google/go-cmdtest"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update test files with results")

func TestCLI(t *testing.T) {
	ts, err := cmdtest.Read("testdata")
	require.NoError(t, err)

	ts.Commands["varlogmr"] = cmdtest.InProcessProgram("varlogmr", func() int {
		app := initCLI()
		err := app.Run(os.Args)
		if err != nil {
			return -1
		}
		return 0
	})
	ts.Run(t, *update)
}
