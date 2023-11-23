package storagenode

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/storagenode/client"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/snpb/mock"
	"github.com/kakao/varlog/proto/varlogpb"
)

const (
	testWaitForDuration = time.Second
	testWaitForTick     = 10 * time.Millisecond
)

func TestNewSimpleStorageNode(t *testing.T, opts ...Option) *StorageNode {
	snOpts := append([]Option{
		WithClusterID(1),
		WithStorageNodeID(1),
		WithVolumes(t.TempDir()),
		WithListenAddress("127.0.0.1:0"),
	}, opts...)
	return TestNewStorageNode(t, snOpts...)
}

func TestNewStorageNode(tb testing.TB, opts ...Option) *StorageNode {
	tb.Helper()
	sn, err := NewStorageNode(opts...)
	assert.NoError(tb, err)
	return sn
}

func TestWaitForStartingOfServe(t *testing.T, sn *StorageNode, timeouts ...time.Duration) {
	waitFor, tick := testWaitForDuration, testWaitForTick
	if len(timeouts) > 0 {
		waitFor = timeouts[0]
	}
	if len(timeouts) > 1 {
		tick = timeouts[1]
	}
	assert.Eventually(t, func() bool {
		sn.mu.Lock()
		defer sn.mu.Unlock()
		return len(sn.advertise) > 0
	}, waitFor, tick)
}

func TestGetStorageNodeID(t *testing.T, sn *StorageNode) types.StorageNodeID {
	return sn.snid
}

func TestGetAdvertiseAddress(t *testing.T, sn *StorageNode) string {
	TestWaitForStartingOfServe(t, sn)
	return sn.advertise
}

func TestNewManagementClient(t *testing.T, cid types.ClusterID, snid types.StorageNodeID, addr string) (*client.ManagementClient, func()) {
	mgr, err := client.NewManager[*client.ManagementClient](client.WithClusterID(cid))
	assert.NoError(t, err)

	mc, err := mgr.GetOrConnect(context.Background(), snid, addr)
	assert.NoError(t, err)

	closer := func() {
		defer func() {
			assert.NoError(t, mgr.Close())
		}()
	}
	return mc, closer
}

func TestGetStorageNodeMetadataDescriptorWithoutAddr(t *testing.T, sn *StorageNode) *snpb.StorageNodeMetadataDescriptor {
	TestWaitForStartingOfServe(t, sn)
	return TestGetStorageNodeMetadataDescriptor(t, sn.cid, sn.snid, sn.advertise)
}

func TestGetStorageNodeMetadataDescriptor(t *testing.T, cid types.ClusterID, snid types.StorageNodeID, addr string) *snpb.StorageNodeMetadataDescriptor {
	snmc, closer := TestNewManagementClient(t, cid, snid, addr)
	defer closer()
	snmd, err := snmc.GetMetadata(context.Background())
	assert.NoError(t, err)
	return snmd
}

func TestAddLogStreamReplica(t *testing.T, cid types.ClusterID, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID, path, addr string) {
	snmc, closer := TestNewManagementClient(t, cid, snid, addr)
	defer closer()
	_, err := snmc.AddLogStreamReplica(context.Background(), tpid, lsid, path)
	assert.NoError(t, err)
}

func TestSealLogStreamReplica(t *testing.T, cid types.ClusterID, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID, lastCommittedGLSN types.GLSN, addr string) (varlogpb.LogStreamStatus, types.GLSN) {
	snmc, closer := TestNewManagementClient(t, cid, snid, addr)
	defer closer()
	status, localHWM, err := snmc.Seal(context.Background(), tpid, lsid, lastCommittedGLSN)
	assert.NoError(t, err)
	return status, localHWM
}

func TestUnsealLogStreamReplica(t *testing.T, cid types.ClusterID, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID, replicas []varlogpb.LogStreamReplica, addr string) {
	snmc, closer := TestNewManagementClient(t, cid, snid, addr)
	defer closer()
	err := snmc.Unseal(context.Background(), tpid, lsid, replicas)
	assert.NoError(t, err)
}

func TestNewLogIOClient(t *testing.T, snid types.StorageNodeID, addr string) (*client.LogClient, func()) {
	mgr, err := client.NewManager[*client.LogClient]()
	assert.NoError(t, err)

	lc, err := mgr.GetOrConnect(context.Background(), snid, addr)
	assert.NoError(t, err)

	closer := func() {
		assert.NoError(t, mgr.Close())
	}
	return lc, closer
}

func TestAppend(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, dataBatch [][]byte, replicas []varlogpb.LogStreamReplica) []snpb.AppendResult {
	lc, closer := TestNewLogIOClient(t, replicas[0].StorageNodeID, replicas[0].Address)
	defer closer()

	res, err := lc.Append(context.Background(), tpid, lsid, dataBatch)
	assert.NoError(t, err)
	return res
}

func TestSubscribe(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, begin, end types.GLSN, snid types.StorageNodeID, addr string) []varlogpb.LogEntry {
	lc, closer := TestNewLogIOClient(t, snid, addr)
	defer closer()

	ch, err := lc.Subscribe(context.Background(), tpid, lsid, begin, end)
	assert.NoError(t, err)

	var les []varlogpb.LogEntry
	for sr := range ch {
		if sr.Error != nil {
			assert.ErrorIs(t, sr.Error, io.EOF)
			break
		}
		assert.NoError(t, sr.Error)
		les = append(les, sr.LogEntry)
	}
	return les
}

func TestSubscribeTo(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, begin, end types.LLSN, snid types.StorageNodeID, addr string) []varlogpb.LogEntry {
	lc, closer := TestNewLogIOClient(t, snid, addr)
	defer closer()

	ch, err := lc.SubscribeTo(context.Background(), tpid, lsid, begin, end)
	assert.NoError(t, err)

	var les []varlogpb.LogEntry
	for sr := range ch {
		if sr.Error != nil {
			assert.ErrorIs(t, sr.Error, io.EOF)
			break
		}
		assert.NoError(t, sr.Error)
		les = append(les, sr.LogEntry)
	}
	return les
}

func TestSync(t *testing.T, cid types.ClusterID, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID, lastGLSN types.GLSN, addr string, dst varlogpb.StorageNode) *snpb.SyncStatus {
	snmc, closer := TestNewManagementClient(t, cid, snid, addr)
	defer closer()

	st, err := snmc.Sync(context.Background(), tpid, lsid, dst.StorageNodeID, dst.Address, lastGLSN)
	assert.NoError(t, err)
	return st
}

func TestTrim(t *testing.T, cid types.ClusterID, snid types.StorageNodeID, tpid types.TopicID, glsn types.GLSN, addr string) map[types.LogStreamID]error {
	snmc, closer := TestNewManagementClient(t, cid, snid, addr)
	defer closer()

	results, err := snmc.Trim(context.Background(), tpid, glsn)
	assert.NoError(t, err)
	return results
}

type testRPCServer struct {
	listener   net.Listener
	grpcServer *grpc.Server
	address    string
	snid       types.StorageNodeID
	wg         sync.WaitGroup

	*mock.MockLogIOServer
	*mock.MockManagementServer
	*mock.MockLogStreamReporterServer
}

var _ snpb.LogIOServer = (*testRPCServer)(nil)
var _ snpb.ManagementServer = (*testRPCServer)(nil)

func TestNewRPCServer(t *testing.T, ctrl *gomock.Controller, snid types.StorageNodeID, address ...string) *testRPCServer {
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

	trs := &testRPCServer{
		listener:                    lis,
		grpcServer:                  rpc.NewServer(),
		address:                     addr,
		snid:                        snid,
		MockLogIOServer:             mock.NewMockLogIOServer(ctrl),
		MockManagementServer:        mock.NewMockManagementServer(ctrl),
		MockLogStreamReporterServer: mock.NewMockLogStreamReporterServer(ctrl),
	}
	snpb.RegisterLogIOServer(trs.grpcServer, trs.MockLogIOServer)
	snpb.RegisterManagementServer(trs.grpcServer, trs.MockManagementServer)
	snpb.RegisterLogStreamReporterServer(trs.grpcServer, trs.MockLogStreamReporterServer)
	return trs
}

func (trs *testRPCServer) Run() {
	trs.wg.Add(1)
	go func() {
		defer trs.wg.Done()
		_ = trs.grpcServer.Serve(trs.listener)
	}()
}

func (trs *testRPCServer) Close() {
	trs.grpcServer.Stop()
	_ = trs.listener.Close()
	trs.wg.Wait()
}

func (trs *testRPCServer) StorageNodeID() types.StorageNodeID {
	return trs.snid
}

func (trs *testRPCServer) Address() string {
	return trs.address
}
