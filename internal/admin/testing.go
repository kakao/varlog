package admin

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/proto/admpb"
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

type TestMockServer struct {
	listener   net.Listener
	grpcServer *grpc.Server
	address    string
	wg         sync.WaitGroup

	*admpb.MockClusterManagerServer
}

var _ admpb.ClusterManagerServer = (*TestMockServer)(nil)

func TestNewMockServer(t *testing.T, ctrl *gomock.Controller) *TestMockServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()

	tms := &TestMockServer{
		listener:                 lis,
		grpcServer:               rpc.NewServer(),
		address:                  addr,
		MockClusterManagerServer: admpb.NewMockClusterManagerServer(ctrl),
	}
	admpb.RegisterClusterManagerServer(tms.grpcServer, tms.MockClusterManagerServer)
	return tms
}

func (tms *TestMockServer) Run() {
	tms.wg.Add(1)
	go func() {
		defer tms.wg.Done()
		_ = tms.grpcServer.Serve(tms.listener)
	}()
}

func (tms *TestMockServer) Close() {
	tms.grpcServer.Stop()
	_ = tms.listener.Close()
	tms.wg.Wait()
}

func (tms *TestMockServer) Address() string {
	return tms.address
}
