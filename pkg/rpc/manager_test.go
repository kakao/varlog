package rpc

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/types"
)

func testNewServer(t *testing.T) (addr string, closer func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	addr = lis.Addr().String()

	server := grpc.NewServer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = server.Serve(lis)
	}()

	return addr, func() {
		server.Stop()
		wg.Wait()
	}
}

func TestManager(t *testing.T) {
	mgr, err := NewManager[types.StorageNodeID]()
	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, mgr.Close())

		// close closed manager
		assert.NoError(t, mgr.Close())
	}()

	addr, closer := testNewServer(t)
	defer closer()

	// new
	conn1, err := mgr.GetOrConnect(context.Background(), 1, addr)
	assert.NoError(t, err)

	// cached
	conn2, err := mgr.GetOrConnect(context.Background(), 1, addr)
	assert.NoError(t, err)
	assert.Equal(t, conn1, conn2)

	// unexpected addr
	_, err = mgr.GetOrConnect(context.Background(), 1, addr+"0")
	assert.Error(t, err)

	// failed connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = mgr.GetOrConnect(ctx, 2, "bad-address", grpc.WithBlock())
	assert.Error(t, err)
}

func TestManagerBadConfig(t *testing.T) {
	_, err := NewManager[types.StorageNodeID](WithLogger(nil))
	assert.Error(t, err)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
