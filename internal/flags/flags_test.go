package flags

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestReplicationFactor(t *testing.T) {
	tcs := []struct {
		name string
		args []string
		ok   bool
	}{
		{
			name: "replication_factor=1",
			args: []string{"test", "--replication-factor=1"},
			ok:   true,
		},
		{
			name: "replication_factor=0",
			args: []string{"test", "--replication-factor=0"},
			ok:   false,
		},
		{
			name: "replication_factor=-1",
			args: []string{"test", "--replication-factor=-1"},
			ok:   false,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			app := &cli.App{
				Name: "test",
				Flags: []cli.Flag{
					ReplicationFactor,
				},
				Writer: io.Discard,
			}
			err := app.Run(tc.args)
			if !tc.ok {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
