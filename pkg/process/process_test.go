package process_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/process"
)

func TestProcess(t *testing.T) {
	tcs := []struct {
		name  string
		testf func(*testing.T)
	}{
		{
			name: "ExitZero",
			testf: func(t *testing.T) {
				p, err := process.New(context.Background(), "sh", "-c", "ls", "-a")
				require.NoError(t, err)
				require.NotEmpty(t, p.String())

				require.NoError(t, p.Start())
				defer func() {
					assert.NoError(t, p.Wait())
				}()
			},
		},
		{
			name: "ExitNonZero",
			testf: func(t *testing.T) {
				p, err := process.New(context.Background(), "sh", "-c", "exit 1")
				require.NoError(t, err)

				require.NoError(t, p.Start())
				defer func() {
					assert.Error(t, p.Wait())
				}()
			},
		},
		{
			name: "InvalidArgument",
			testf: func(t *testing.T) {
				p, err := process.New(context.Background(), "")
				require.NoError(t, err)

				require.Error(t, p.Start())
			},
		},
		{
			name: "ContextTimeout",
			testf: func(*testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()

				p, err := process.New(ctx, "sh", "-c", "yes")
				require.NoError(t, err)

				require.NoError(t, p.Start())
				require.Error(t, p.Wait())
			},
		},
		{
			name: "Stop",
			testf: func(*testing.T) {
				p, err := process.New(context.Background(), "sh", "-c", "yes")
				require.NoError(t, err)

				require.NoError(t, p.Start())
				defer func() {
					require.Error(t, p.Wait())
				}()

				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					for i := 0; i < 100; i++ {
						<-p.Stdout()
					}
					p.Stop()

				}()
				go func() {
					defer wg.Done()
					for stderr := range p.Stderr() {
						assert.Failf(t, "unexpected stderr", "stderr: %s", stderr)
						return
					}
				}()
				wg.Wait()
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, tc.testf)
	}
}
