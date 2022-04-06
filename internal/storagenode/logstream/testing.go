package logstream

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/snpb/mock"
)

type testReplicateServer struct {
	server *grpc.Server
	wg     sync.WaitGroup
	mock   *mock.MockReplicatorServer
}

var _ snpb.ReplicatorServer = (*testReplicateServer)(nil)

func (trs *testReplicateServer) ReplicateDeprecated(stream snpb.Replicator_ReplicateDeprecatedServer) error {
	return trs.mock.ReplicateDeprecated(stream)
}

func (trs *testReplicateServer) Replicate(stream snpb.Replicator_ReplicateServer) error {
	return trs.mock.Replicate(stream)
}

func (trs *testReplicateServer) SyncInit(ctx context.Context, req *snpb.SyncInitRequest) (*snpb.SyncInitResponse, error) {
	return trs.mock.SyncInit(ctx, req)
}

func (trs *testReplicateServer) SyncReplicate(ctx context.Context, req *snpb.SyncReplicateRequest) (*snpb.SyncReplicateResponse, error) {
	return trs.mock.SyncReplicate(ctx, req)
}

func TestNewReplicateServer(t *testing.T, mock *mock.MockReplicatorServer) (server snpb.ReplicatorServer, rpcConn *rpc.Conn, closer func()) {
	trs := &testReplicateServer{
		server: grpc.NewServer(),
		mock:   mock,
	}

	snpb.RegisterReplicatorServer(trs.server, trs)

	lis, connect := rpc.TestNewConn(t, context.Background(), 1<<10)
	trs.wg.Add(1)
	go func() {
		defer trs.wg.Done()
		_ = trs.server.Serve(lis)
	}()

	rpcConn = connect()

	closer = func() {
		assert.NoError(t, rpcConn.Close())
		assert.NoError(t, lis.Close())
		trs.server.GracefulStop()
		trs.wg.Wait()
	}
	return trs, rpcConn, closer
}

func TestNewBatchData(tb testing.TB, batchLen int, msgSize int) [][]byte {
	tb.Helper()
	batch := make([][]byte, batchLen)
	for i := 0; i < batchLen; i++ {
		batch[i] = make([]byte, msgSize)
		for j := 0; j < msgSize; j++ {
			batch[i][j] = '.'
		}
	}
	return batch
}
