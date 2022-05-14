package client_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/kakao/varlog/internal/storagenode/client"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type testServer struct {
	listener   net.Listener
	grpcServer *grpc.Server
	address    string
	snid       types.StorageNodeID
	wg         sync.WaitGroup
}

var _ snpb.LogIOServer = (*testServer)(nil)
var _ snpb.ManagementServer = (*testServer)(nil)

func newTestServer(t *testing.T, snid types.StorageNodeID, address ...string) *testServer {
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

	tls := &testServer{
		listener:   lis,
		grpcServer: grpc.NewServer(),
		address:    addr,
		snid:       snid,
	}
	snpb.RegisterLogIOServer(tls.grpcServer, tls)
	snpb.RegisterManagementServer(tls.grpcServer, tls)
	return tls
}

func (tls *testServer) run() {
	tls.wg.Add(1)
	go func() {
		defer tls.wg.Done()
		_ = tls.grpcServer.Serve(tls.listener)
	}()
}

func (tls *testServer) close() {
	tls.grpcServer.Stop()
	_ = tls.listener.Close()
	tls.wg.Wait()
}

func (tls *testServer) Append(context.Context, *snpb.AppendRequest) (*snpb.AppendResponse, error) {
	panic("not implemented")
}

func (tls *testServer) Read(context.Context, *snpb.ReadRequest) (*snpb.ReadResponse, error) {
	panic("not implemented")
}

func (tls *testServer) Subscribe(*snpb.SubscribeRequest, snpb.LogIO_SubscribeServer) error {
	panic("not implemented")
}

func (tls *testServer) SubscribeTo(*snpb.SubscribeToRequest, snpb.LogIO_SubscribeToServer) error {
	panic("not implemented")
}

func (tls *testServer) TrimDeprecated(context.Context, *snpb.TrimDeprecatedRequest) (*pbtypes.Empty, error) {
	panic("not implemented")
}

func (tls *testServer) LogStreamMetadata(context.Context, *snpb.LogStreamMetadataRequest) (*snpb.LogStreamMetadataResponse, error) {
	panic("not implemented")
}

func (tls *testServer) GetMetadata(context.Context, *snpb.GetMetadataRequest) (*snpb.GetMetadataResponse, error) {
	return &snpb.GetMetadataResponse{
		StorageNodeMetadata: &snpb.StorageNodeMetadataDescriptor{
			StorageNode: &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: tls.snid,
					Address:       tls.address,
				},
			},
		},
	}, nil
}

func (tls *testServer) AddLogStreamReplica(context.Context, *snpb.AddLogStreamReplicaRequest) (*snpb.AddLogStreamReplicaResponse, error) {
	panic("not implemented")
}

func (tls *testServer) RemoveLogStream(context.Context, *snpb.RemoveLogStreamRequest) (*pbtypes.Empty, error) {
	panic("not implemented")
}

func (tls *testServer) Seal(context.Context, *snpb.SealRequest) (*snpb.SealResponse, error) {
	panic("not implemented")
}

func (tls *testServer) Unseal(context.Context, *snpb.UnsealRequest) (*pbtypes.Empty, error) {
	panic("not implemented")
}

func (tls *testServer) Sync(context.Context, *snpb.SyncRequest) (*snpb.SyncResponse, error) {
	panic("not implemented")
}

func (tls *testServer) Trim(context.Context, *snpb.TrimRequest) (*snpb.TrimResponse, error) {
	panic("not implemented")
}

func TestManager_InvalidConfig(t *testing.T) {
	_, err := client.NewManager[*client.LogClient](client.WithLogger(nil))
	assert.Error(t, err)
}

func TestManager_CloseClosed(t *testing.T) {
	mgr, err := client.NewManager[*client.LogClient]()
	assert.NoError(t, err)

	assert.NoError(t, mgr.Close())
	assert.NoError(t, mgr.Close())
}

func TestManager_UnreachableServer(t *testing.T) {
	mgr, err := client.NewManager[*client.LogClient](
		client.WithDefaultGRPCDialOptions(
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
	tls1 := newTestServer(t, 1)
	tls1.run()
	defer tls1.close()

	// server2
	tls2 := newTestServer(t, 2)
	tls2.run()
	defer tls2.close()

	mgr, err := client.NewManager[*client.LogClient]()
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

	other, err := mgr.GetOrConnect(context.Background(), tls2.snid, tls2.address)
	assert.NoError(t, err)
	assert.NotSame(t, other, client2)

	assert.NoError(t, mgr.Close())

	_, err = mgr.Get(tls1.snid)
	assert.Error(t, err)

	_, err = mgr.GetOrConnect(context.Background(), tls1.snid, tls1.address)
	assert.Error(t, err)
}

func TestManager_ServerRestart(t *testing.T) {
	tls := newTestServer(t, 1)
	tls.run()

	connectParams := grpc.ConnectParams{
		Backoff:           backoff.DefaultConfig,
		MinConnectTimeout: 20 * time.Second, // grpc default
	}
	connectParams.Backoff.MaxDelay = 2 * time.Second
	mgr, err := client.NewManager[*client.ManagementClient](
		client.WithDefaultGRPCDialOptions(
			grpc.WithConnectParams(connectParams),
		),
	)
	assert.NoError(t, err)
	defer func() {
		err := mgr.Close()
		assert.NoError(t, err)
	}()

	client, err := mgr.GetOrConnect(context.Background(), tls.snid, tls.address)
	assert.NoError(t, err)

	snmd, err := client.GetMetadata(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, client.Target(), snmd.StorageNode.StorageNode)

	tls.close()
	_, err = client.GetMetadata(context.Background())
	assert.Error(t, err)

	tls = newTestServer(t, 1, tls.address)
	tls.run()
	defer tls.close()

	assert.Eventually(t, func() bool {
		snmd, err := client.GetMetadata(context.Background())
		return err == nil && client.Target() == snmd.StorageNode.StorageNode
	}, connectParams.Backoff.MaxDelay*2, 100*time.Millisecond)
}
