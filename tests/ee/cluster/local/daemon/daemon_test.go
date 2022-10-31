//go:build !e2e

package daemon_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/tests/ee/cluster/local/daemon"
)

func TestDaemon(t *testing.T) {
	tcs := []struct {
		name  string
		testf func(*testing.T)
	}{
		{
			name: "ExitZero",
			testf: func(t *testing.T) {
				p, err := daemon.New("sh", daemon.WithArguments("-c", "ls", "-a"))
				require.NoError(t, err)
				require.NotEmpty(t, p.String())
				require.NoError(t, p.Run())
			},
		},
		{
			name: "ExitNonZero",
			testf: func(t *testing.T) {
				p, err := daemon.New("sh", daemon.WithArguments("-c", "exit 1"))
				require.NoError(t, err)
				require.Error(t, p.Run())
			},
		},
		{
			name: "InvalidArgument",
			testf: func(t *testing.T) {
				p, err := daemon.New("")
				require.NoError(t, err)
				require.Error(t, p.Run())
			},
		},
		{
			name: "Stop",
			testf: func(*testing.T) {
				outputChan := make(chan string)
				p, err := daemon.New(
					"sh",
					daemon.WithArguments("-c", "yes"),
					daemon.WithOutputChannel(outputChan),
				)
				require.NoError(t, err)

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					require.Error(t, p.Run())
				}()

				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 100; i++ {
						<-outputChan
					}
					p.Stop()

				}()
				wg.Wait()
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, tc.testf)
	}
}
