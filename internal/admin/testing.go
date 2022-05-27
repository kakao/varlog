package admin

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestServer struct {
	*Admin
	wg sync.WaitGroup
}

func TestNewClusterManager(t *testing.T, opts ...Option) *TestServer {
	t.Helper()
	adm, err := New(context.Background(), opts...)
	assert.NoError(t, err)
	return &TestServer{Admin: adm}
}

func (ts *TestServer) Serve(t *testing.T) {
	t.Helper()
	ts.wg.Add(1)
	go func() {
		defer ts.wg.Done()
		err := ts.Admin.Serve()
		assert.NoError(t, err)
	}()

	assert.Eventually(t, func() bool {
		return len(ts.Admin.Address()) > 0
	}, time.Second, 10*time.Millisecond)
}

func (ts *TestServer) Close(t *testing.T) {
	t.Helper()
	err := ts.Admin.Close()
	assert.NoError(t, err)
	ts.wg.Wait()
}
