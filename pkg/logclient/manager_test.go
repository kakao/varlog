package logclient_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/pkg/logclient"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

type testLogServer struct {
	listener   net.Listener
	grpcServer *grpc.Server
	address    string
	snid       types.StorageNodeID
	wg         sync.WaitGroup
}

var _ snpb.LogIOServer = (*testLogServer)(nil)

func newTestLogServer(t *testing.T, snid types.StorageNodeID, address ...string) *testLogServer {
	t.Helper()

	var addr string
	if len(address) == 0 {
		addr = "127.0.0.1:0"
	} else {
		addr = address[0]
	}
	lis, err := net.Listen("tcp", addr)
	assert.NoError(t, err)
	addr = lis.Addr().String()

	tls := &testLogServer{
		listener:   lis,
		grpcServer: grpc.NewServer(),
		address:    addr,
		snid:       snid,
	}
	snpb.RegisterLogIOServer(tls.grpcServer, tls)
	return tls
}

func (tls *testLogServer) run(t *testing.T) {
	tls.wg.Add(1)
	go func() {
		defer tls.wg.Done()
		_ = tls.grpcServer.Serve(tls.listener)
	}()
}

func (tls *testLogServer) close() {
	tls.grpcServer.Stop()
	_ = tls.listener.Close()
	tls.wg.Wait()
}

func (tls *testLogServer) Append(context.Context, *snpb.AppendRequest) (*snpb.AppendResponse, error) {
	panic("not implemented")
}

func (tls *testLogServer) Read(context.Context, *snpb.ReadRequest) (*snpb.ReadResponse, error) {
	panic("not implemented")
}

func (tls *testLogServer) Subscribe(*snpb.SubscribeRequest, snpb.LogIO_SubscribeServer) error {
	panic("not implemented")
}

func (tls *testLogServer) SubscribeTo(*snpb.SubscribeToRequest, snpb.LogIO_SubscribeToServer) error {
	panic("not implemented")
}

func (tls *testLogServer) TrimDeprecated(context.Context, *snpb.TrimDeprecatedRequest) (*pbtypes.Empty, error) {
	panic("not implemented")
}

func (tls *testLogServer) LogStreamMetadata(context.Context, *snpb.LogStreamMetadataRequest) (*snpb.LogStreamMetadataResponse, error) {
	panic("not implemented")
}

func TestManager_InvalidConfig(t *testing.T) {
	_, err := logclient.NewManager[*logclient.Client](logclient.WithLogger(nil))
	assert.Error(t, err)
}

func TestManager_CloseClosed(t *testing.T) {
	mgr, err := logclient.NewManager[*logclient.Client]()
	assert.NoError(t, err)

	assert.NoError(t, mgr.Close())
	assert.NoError(t, mgr.Close())
}

func TestManager_UnreachableServer(t *testing.T) {
	mgr, err := logclient.NewManager[*logclient.Client](
		logclient.WithDefaultGRPCDialOptions(
			grpc.WithBlock(),
			grpc.FailOnNonTempDialError(true),
		),
	)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = mgr.GetOrConnect(ctx, 1, "127.0.0.1:0")
	assert.Error(t, err)
}

func TestManager_Client(t *testing.T) {
	// server1
	tls1 := newTestLogServer(t, 1)
	tls1.run(t)
	defer tls1.close()

	// server2
	tls2 := newTestLogServer(t, 2)
	tls2.run(t)
	defer tls2.close()

	mgr, err := logclient.NewManager[*logclient.Client]()
	assert.NoError(t, err)

	_, err = mgr.Get(tls1.snid)
	assert.Error(t, err)

	client1, err := mgr.GetOrConnect(context.Background(), tls1.snid, tls1.address)
	assert.NoError(t, err)

	client2, err := mgr.Get(tls1.snid)
	assert.NoError(t, err)
	assert.Same(t, client1, client2)

	client2, err = mgr.GetOrConnect(context.Background(), tls1.snid, tls1.address)
	assert.NoError(t, err)
	assert.Same(t, client1, client2)

	_, err = mgr.GetOrConnect(context.Background(), tls1.snid, tls2.address)
	assert.Error(t, err)

	client2, err = mgr.GetOrConnect(context.Background(), tls2.snid, tls2.address)
	assert.NoError(t, err)
	assert.NotSame(t, client1, client2)

	err = mgr.CloseClient(tls2.snid)
	assert.NoError(t, err)

	_, err = mgr.Get(tls2.snid)
	assert.Error(t, err)

	client2Other, err := mgr.GetOrConnect(context.Background(), tls2.snid, tls2.address)
	assert.NoError(t, err)
	assert.NotSame(t, client2Other, client2)

	assert.NoError(t, mgr.Close())

	_, err = mgr.Get(tls1.snid)
	assert.Error(t, err)

	_, err = mgr.GetOrConnect(context.Background(), tls1.snid, tls1.address)
	assert.Error(t, err)
}
