package rpc

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/pkg/types"
)

func testNewServer(t *testing.T) (addr string, closer func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	addr = lis.Addr().String()

	server := NewServer()

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
	tcs := []struct {
		name  string
		testf func(t *testing.T, mgr *Manager[types.StorageNodeID], serverAddr string)
	}{
		{
			name: "NewConn",
			testf: func(t *testing.T, mgr *Manager[types.StorageNodeID], serverAddr string) {
				_, err := mgr.GetOrConnect(context.Background(), 1, serverAddr)
				require.NoError(t, err)
			},
		},
		{
			name: "GetConn",
			testf: func(t *testing.T, mgr *Manager[types.StorageNodeID], serverAddr string) {
				conn1, err := mgr.GetOrConnect(context.Background(), 1, serverAddr)
				require.NoError(t, err)

				conn2, err := mgr.GetOrConnect(context.Background(), 1, serverAddr)
				require.NoError(t, err)

				require.Equal(t, conn1, conn2)
			},
		},
		{
			name: "UnexpectedAddr",
			testf: func(t *testing.T, mgr *Manager[types.StorageNodeID], serverAddr string) {
				_, err := mgr.GetOrConnect(context.Background(), 1, serverAddr)
				require.NoError(t, err)

				_, err = mgr.GetOrConnect(context.Background(), 1, serverAddr+"0")
				require.Error(t, err)
			},
		},
		{
			name: "UnreachableServer",
			testf: func(t *testing.T, mgr *Manager[types.StorageNodeID], serverAddr string) {
				_, err := mgr.GetOrConnect(context.Background(), 1, "bad-address")
				require.NoError(t, err)
			},
		},
		{
			name: "CloseUnknownID",
			testf: func(t *testing.T, mgr *Manager[types.StorageNodeID], serverAddr string) {
				err := mgr.CloseClient(2)
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			mgr, err := NewManager[types.StorageNodeID]()
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, mgr.Close())
			})

			addr1, closer1 := testNewServer(t)
			t.Cleanup(closer1)

			tc.testf(t, mgr, addr1)
		})
	}
}

func TestManagerBadConfig(t *testing.T) {
	_, err := NewManager[types.StorageNodeID](WithLogger(nil))
	assert.Error(t, err)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
