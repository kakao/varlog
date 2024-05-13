package main

import (
	"flag"
	"testing"

	"github.com/google/go-cmdtest"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update test files with results")

func TestCLI(t *testing.T) {
	ts, err := cmdtest.Read("testdata")
	require.NoError(t, err)

	ts.Commands["varlogsn"] = cmdtest.InProcessProgram("varlogsn", run)
	ts.Run(t, *update)
}
