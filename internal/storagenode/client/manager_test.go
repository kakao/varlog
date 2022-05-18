package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/client"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// server1
	tls1 := storagenode.TestNewRPCServer(t, ctrl, 1)
	tls1.Run()
	defer tls1.Close()

	// server2
	tls2 := storagenode.TestNewRPCServer(t, ctrl, 2)
	tls2.Run()
	defer tls2.Close()

	mgr, err := client.NewManager[*client.LogClient]()
	assert.NoError(t, err)

	_, err = mgr.Get(tls1.StorageNodeID())
	assert.Error(t, err)

	_, err = mgr.GetByAddress(tls1.Address())
	assert.Error(t, err)

	client1, err := mgr.GetOrConnect(context.Background(), tls1.StorageNodeID(), tls1.Address())
	assert.NoError(t, err)

	client2, err := mgr.Get(tls1.StorageNodeID())
	assert.NoError(t, err)
	assert.Same(t, client1, client2)

	client2, err = mgr.GetByAddress(tls1.Address())
	assert.NoError(t, err)
	assert.Same(t, client1, client2)

	client2, err = mgr.GetOrConnect(context.Background(), tls1.StorageNodeID(), tls1.Address())
	assert.NoError(t, err)
	assert.Same(t, client1, client2)

	_, err = mgr.GetOrConnect(context.Background(), tls1.StorageNodeID(), tls2.Address())
	assert.Error(t, err)

	client2, err = mgr.GetOrConnect(context.Background(), tls2.StorageNodeID(), tls2.Address())
	assert.NoError(t, err)
	assert.NotSame(t, client1, client2)

	err = mgr.CloseClient(tls2.StorageNodeID())
	assert.NoError(t, err)

	_, err = mgr.Get(tls2.StorageNodeID())
	assert.Error(t, err)

	_, err = mgr.GetByAddress(tls2.Address())
	assert.Error(t, err)

	other, err := mgr.GetOrConnect(context.Background(), tls2.StorageNodeID(), tls2.Address())
	assert.NoError(t, err)
	assert.NotSame(t, other, client2)

	assert.NoError(t, mgr.Close())

	_, err = mgr.Get(tls1.StorageNodeID())
	assert.Error(t, err)

	_, err = mgr.GetByAddress(tls1.Address())
	assert.Error(t, err)

	_, err = mgr.GetOrConnect(context.Background(), tls1.StorageNodeID(), tls1.Address())
	assert.Error(t, err)
}

func TestManager_ServerRestart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tls := storagenode.TestNewRPCServer(t, ctrl, 1)
	tls.MockManagementServer.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).Return(
		&snpb.GetMetadataResponse{
			StorageNodeMetadata: &snpb.StorageNodeMetadataDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: tls.StorageNodeID(),
					Address:       tls.Address(),
				},
			},
		},
		nil,
	).AnyTimes()
	tls.Run()

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
		assert.NoError(t, mgr.Close())
	}()

	mc, err := mgr.GetOrConnect(context.Background(), tls.StorageNodeID(), tls.Address())
	assert.NoError(t, err)

	snmd, err := mc.GetMetadata(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, mc.Target(), snmd.StorageNode)

	tls.Close()
	_, err = mc.GetMetadata(context.Background())
	assert.Error(t, err)

	tls = storagenode.TestNewRPCServer(t, ctrl, 1, tls.Address())
	tls.MockManagementServer.EXPECT().GetMetadata(gomock.Any(), gomock.Any()).Return(
		&snpb.GetMetadataResponse{
			StorageNodeMetadata: &snpb.StorageNodeMetadataDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: tls.StorageNodeID(),
					Address:       tls.Address(),
				},
			},
		},
		nil,
	).AnyTimes()
	tls.Run()
	defer tls.Close()

	assert.Eventually(t, func() bool {
		snmd, err := mc.GetMetadata(context.Background())
		return err == nil && mc.Target() == snmd.StorageNode
	}, connectParams.Backoff.MaxDelay*2, 100*time.Millisecond)
}
