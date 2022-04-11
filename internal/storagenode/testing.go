package storagenode

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/logc"
	"github.daumkakao.com/varlog/varlog/pkg/snc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
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

func TestNewManagementClient(t *testing.T, cid types.ClusterID, addr string) (snc.StorageNodeManagementClient, func()) {
	snmc, err := snc.NewManagementClient(context.Background(), cid, addr, zap.NewNop())
	assert.NoError(t, err)
	closer := func() {
		defer func() {
			assert.NoError(t, snmc.Close())
		}()
	}
	return snmc, closer
}

func TestGetStorageNodeMetadataDescriptorWithoutAddr(t *testing.T, sn *StorageNode) *snpb.StorageNodeMetadataDescriptor {
	TestWaitForStartingOfServe(t, sn)
	return TestGetStorageNodeMetadataDescriptor(t, sn.cid, sn.advertise)
}

func TestGetStorageNodeMetadataDescriptor(t *testing.T, cid types.ClusterID, addr string) *snpb.StorageNodeMetadataDescriptor {
	snmc, closer := TestNewManagementClient(t, cid, addr)
	defer closer()
	snmd, err := snmc.GetMetadata(context.Background())
	assert.NoError(t, err)
	return snmd
}

func TestAddLogStreamReplica(t *testing.T, cid types.ClusterID, tpid types.TopicID, lsid types.LogStreamID, path, addr string) {
	snmc, closer := TestNewManagementClient(t, cid, addr)
	defer closer()
	err := snmc.AddLogStreamReplica(context.Background(), tpid, lsid, path)
	assert.NoError(t, err)
}

func TestSealLogStreamReplica(t *testing.T, cid types.ClusterID, tpid types.TopicID, lsid types.LogStreamID, lastCommittedGLSN types.GLSN, addr string) (varlogpb.LogStreamStatus, types.GLSN) {
	snmc, closer := TestNewManagementClient(t, cid, addr)
	defer closer()
	status, localHWM, err := snmc.Seal(context.Background(), tpid, lsid, lastCommittedGLSN)
	assert.NoError(t, err)
	return status, localHWM
}

func TestUnsealLogStreamReplica(t *testing.T, cid types.ClusterID, tpid types.TopicID, lsid types.LogStreamID, replicas []varlogpb.LogStreamReplica, addr string) {
	snmc, closer := TestNewManagementClient(t, cid, addr)
	defer closer()
	err := snmc.Unseal(context.Background(), tpid, lsid, replicas)
	assert.NoError(t, err)
}

func TestNewLogIOClient(t *testing.T, addr string) (logc.LogIOClient, func()) {
	client, err := logc.NewLogIOClient(context.Background(), addr)
	assert.NoError(t, err)
	closer := func() {
		assert.NoError(t, client.Close())
	}
	return client, closer
}

func TestAppend(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, dataBatch [][]byte, replicas []varlogpb.LogStreamReplica) []snpb.AppendResult {
	client, closer := TestNewLogIOClient(t, replicas[0].Address)
	defer closer()

	var backups []varlogpb.StorageNode
	for _, replica := range replicas[1:] {
		backups = append(backups, varlogpb.StorageNode{
			StorageNodeID: replica.StorageNodeID,
			Address:       replica.Address,
		})
	}
	res, err := client.Append(context.Background(), tpid, lsid, dataBatch, backups...)
	assert.NoError(t, err)
	return res
}

func TestSubscribe(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, begin, end types.GLSN, addr string) []varlogpb.LogEntry {
	client, closer := TestNewLogIOClient(t, addr)
	defer closer()

	ch, err := client.Subscribe(context.Background(), tpid, lsid, begin, end)
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

func TestSubscribeTo(t *testing.T, tpid types.TopicID, lsid types.LogStreamID, begin, end types.LLSN, addr string) []varlogpb.LogEntry {
	client, closer := TestNewLogIOClient(t, addr)
	defer closer()

	ch, err := client.SubscribeTo(context.Background(), tpid, lsid, begin, end)
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

func TestSync(t *testing.T, cid types.ClusterID, tpid types.TopicID, lsid types.LogStreamID, lastGLSN types.GLSN, addr string, dst varlogpb.StorageNode) *snpb.SyncStatus {
	snmc, closer := TestNewManagementClient(t, cid, addr)
	defer closer()

	st, err := snmc.Sync(context.Background(), tpid, lsid, dst.StorageNodeID, dst.Address, lastGLSN)
	assert.NoError(t, err)
	return st
}

func TestTrim(t *testing.T, cid types.ClusterID, tpid types.TopicID, glsn types.GLSN, addr string) map[types.LogStreamID]error {
	snmc, closer := TestNewManagementClient(t, cid, addr)
	defer closer()

	results, err := snmc.Trim(context.Background(), tpid, glsn)
	assert.NoError(t, err)
	return results
}
