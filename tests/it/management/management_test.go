package management

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kakao/varlog/internal/admin"
	"github.com/kakao/varlog/internal/admin/snwatcher"
	"github.com/kakao/varlog/internal/metarepos"
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/tests/it"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreTopFunction(
		"go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop",
	))
}

func TestStorageNode_Heartbeat(t *testing.T) {
	const (
		tick            = time.Second
		reportInterval  = tick
		hbCheckDeadline = tick
		hbTimeout       = 5 * tick
	)

	clus := it.NewVarlogCluster(t,
		it.WithNumberOfStorageNodes(1),
		it.WithVMSOptions(
			admin.WithStorageNodeWatcherOptions(
				snwatcher.WithTick(tick),
				snwatcher.WithReportInterval(reportInterval),
				snwatcher.WithHeartbeatCheckDeadline(hbCheckDeadline),
				snwatcher.WithHeartbeatTimeout(hbTimeout),
			),
		),
	)
	defer clus.Close(t)

	adm := clus.GetVMSClient(t)
	snid := clus.StorageNodeIDAtIndex(t, 0)

	snms, err := adm.ListStorageNodes(context.Background())
	assert.NoError(t, err)
	assert.Len(t, snms, 1)
	assert.False(t, snms[0].LastHeartbeatTime.IsZero())

	assert.Eventually(t, func() bool {
		snm, err := adm.GetStorageNode(context.Background(), snid)
		assert.NoError(t, err)
		return snm.LastHeartbeatTime.After(snms[0].LastHeartbeatTime)
	}, 10*tick, tick)
}

func TestStorageNode_HeartbeatFailure(t *testing.T) {
	const (
		tick            = time.Second
		reportInterval  = tick
		hbCheckDeadline = tick
		hbTimeout       = 5 * tick
	)

	clus := it.NewVarlogCluster(t,
		it.WithNumberOfStorageNodes(1),
		it.WithVMSOptions(
			admin.WithStorageNodeWatcherOptions(
				snwatcher.WithTick(tick),
				snwatcher.WithReportInterval(reportInterval),
				snwatcher.WithHeartbeatCheckDeadline(hbCheckDeadline),
				snwatcher.WithHeartbeatTimeout(hbTimeout),
			),
		),
	)
	defer clus.Close(t)

	adm := clus.GetVMSClient(t)
	snid := clus.StorageNodeIDAtIndex(t, 0)

	clus.CloseSN(t, snid)

	snm, err := adm.GetStorageNode(context.Background(), snid)
	assert.NoError(t, err)
	oldts := snm.LastHeartbeatTime

	time.Sleep(10 * tick)

	snm, err = adm.GetStorageNode(context.Background(), snid)
	assert.NoError(t, err)
	newts := snm.LastHeartbeatTime

	assert.Equal(t, oldts, newts)
}

func TestUnregisterInactiveStorageNode(t *testing.T) {
	clus := it.NewVarlogCluster(t, it.WithNumberOfStorageNodes(1))
	defer clus.Close(t)

	snID := clus.StorageNodeIDAtIndex(t, 0)
	err := clus.GetVMSClient(t).UnregisterStorageNode(context.Background(), snID)
	require.NoError(t, err)

	snMap, err := clus.GetVMSClient(t).ListStorageNodes(context.Background())
	require.NoError(t, err)
	require.Len(t, snMap, 0)
}

func TestUnregisterActiveStorageNode(t *testing.T) {
	clus := it.NewVarlogCluster(t, it.WithNumberOfStorageNodes(1), it.WithNumberOfLogStreams(1), it.WithNumberOfTopics(1))
	defer clus.Close(t)

	snID := clus.StorageNodeIDAtIndex(t, 0)
	err := clus.GetVMSClient(t).UnregisterStorageNode(context.Background(), snID)
	require.Error(t, err)
}

func TestAddAlreadyExistedStorageNode(t *testing.T) {
	clus := it.NewVarlogCluster(t, it.WithNumberOfStorageNodes(1))
	defer clus.Close(t)

	snID := clus.StorageNodeIDAtIndex(t, 0)
	addr := clus.SNClientOf(t, snID).Target().Address
	_, err := clus.GetVMSClient(t).AddStorageNode(context.TODO(), snID, addr)
	require.NoError(t, err)
}

func TestUnregisterLogStream(t *testing.T) {
	clus := it.NewVarlogCluster(t, it.WithNumberOfStorageNodes(1), it.WithNumberOfLogStreams(1), it.WithNumberOfTopics(1))
	defer clus.Close(t)

	topicID := clus.TopicIDs()[0]
	lsID := clus.LogStreamIDs(topicID)[0]
	err := clus.GetVMSClient(t).UnregisterLogStream(context.Background(), topicID, lsID)
	require.Error(t, err)

	_, err = clus.GetVMSClient(t).Seal(context.Background(), topicID, lsID)
	require.NoError(t, err)

	err = clus.GetVMSClient(t).UnregisterLogStream(context.Background(), topicID, lsID)
	require.NoError(t, err)
}

func TestAddLogStream_LogStreamsCount(t *testing.T) {
	const (
		replicationFactor  = 1
		storageNodeCount   = 1
		topicCount         = 1
		maxLogStreamsCount = 2
	)

	tcs := []struct {
		name   string
		mrOpts []metarepos.Option
		snOpts []storagenode.Option
	}{
		{
			name: "StorageNodeLimit",
			snOpts: []storagenode.Option{
				storagenode.WithMaxLogStreamReplicasCount(maxLogStreamsCount),
			},
		},
		{
			name: "MetadataRepositoryLimit",
			mrOpts: []metarepos.Option{
				metarepos.WithMaxLogStreamsCountPerTopic(maxLogStreamsCount),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			clus := it.NewVarlogCluster(t,
				it.WithReplicationFactor(replicationFactor),
				it.WithNumberOfStorageNodes(storageNodeCount),
				it.WithNumberOfTopics(topicCount),
				it.WithCustomizedMetadataRepositoryOptions(tc.mrOpts...),
				it.WithCustomizedStorageNodeOptions(tc.snOpts...),
			)
			defer clus.Close(t)

			ctx := context.Background()
			tpid := clus.TopicIDs()[0]
			mc := clus.GetVMSClient(t)

			for i := 0; i < maxLogStreamsCount; i++ {
				_, err := mc.AddLogStream(ctx, tpid, nil)
				require.NoError(t, err)
			}

			_, err := mc.AddLogStream(ctx, tpid, nil)
			require.Error(t, err)
			require.Equal(t, codes.ResourceExhausted, status.Code(err))
		})
	}
}

func TestAddLogStreamWithNotExistedNode(t *testing.T) {
	clus := it.NewVarlogCluster(t, it.WithNumberOfTopics(1))
	defer clus.Close(t)

	replicas := []*varlogpb.ReplicaDescriptor{
		{
			StorageNodeID:   types.StorageNodeID(1),
			StorageNodePath: "/fake",
		},
	}
	topicID := clus.TopicIDs()[0]
	_, err := clus.GetVMSClient(t).AddLogStream(context.Background(), topicID, replicas)
	require.Error(t, err)
}

func TestAddLogStreamManually(t *testing.T) {
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfTopics(1),
	)
	defer clus.Close(t)

	replicas := make([]*varlogpb.ReplicaDescriptor, 0, clus.ReplicationFactor())
	for snID := range clus.StorageNodes() {
		snmd, err := clus.SNClientOf(t, snID).GetMetadata(context.Background())
		require.NoError(t, err)

		replicas = append(replicas, &varlogpb.ReplicaDescriptor{
			StorageNodeID:   snID,
			StorageNodePath: snmd.Storages[0].Path,
		})
	}

	topicID := clus.TopicIDs()[0]
	_, err := clus.GetVMSClient(t).AddLogStream(context.Background(), topicID, replicas)
	require.NoError(t, err)
}

func TestAddLogStreamPartiallyRegistered(t *testing.T) {
	const lsID = types.LogStreamID(1)

	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfTopics(1),
	)
	defer clus.Close(t)

	// NOTE: Log stream whose ID is 1 is partially generated.
	// SN1 has a log stream replica whose ID is 1.
	// SN2 has no log stream replica.
	snid1 := clus.StorageNodeIDAtIndex(t, 0)
	sn1 := clus.SNClientOf(t, snid1)
	snmd1, err := sn1.GetMetadata(context.Background())
	require.NoError(t, err)

	topicID := clus.TopicIDs()[0]
	_, err = sn1.AddLogStreamReplica(context.Background(), topicID, lsID, snmd1.GetStorages()[0].GetPath())
	require.NoError(t, err)

	snid2 := clus.StorageNodeIDAtIndex(t, 1)
	sn2 := clus.SNClientOf(t, snid2)
	snmd2, err := sn2.GetMetadata(context.Background())
	require.NoError(t, err)

	// NOTE: VMS tries to create new log stream replica, and its ID will be 1.
	// But the log stream was generated partially.
	replicas := []*varlogpb.ReplicaDescriptor{
		{
			StorageNodeID:   snid1,
			StorageNodePath: snmd1.GetStorages()[0].GetPath(),
		},
		{
			StorageNodeID:   snid2,
			StorageNodePath: snmd2.GetStorages()[0].GetPath(),
		},
	}
	_, err = clus.GetVMSClient(t).AddLogStream(context.Background(), topicID, replicas)
	require.Error(t, err)

	// Retring add new log stream will be succeed, since VMS refreshes its ID pool.
	_, err = clus.GetVMSClient(t).AddLogStream(context.Background(), topicID, replicas)
	require.NoError(t, err)
}

func TestRemoveLogStreamReplica(t *testing.T) {
	const lsID = types.LogStreamID(1)

	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfTopics(1),
	)
	defer clus.Close(t)

	// Not registered log stream replica: garbage
	snid := clus.StorageNodeIDAtIndex(t, 0)
	sn := clus.SNClientOf(t, snid)
	snmd, err := sn.GetMetadata(context.Background())
	require.NoError(t, err)
	topicID := clus.TopicIDs()[0]
	_, err = sn.AddLogStreamReplica(context.Background(), topicID, lsID, snmd.GetStorages()[0].GetPath())
	require.NoError(t, err)

	err = clus.GetVMSClient(t).RemoveLogStreamReplica(context.TODO(), snid, topicID, lsID)
	require.NoError(t, err)
}

func TestAddLogStreamWithAutoUnseal(t *testing.T) {
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithNumberOfTopics(1),
		it.WithVMSOptions(
			admin.WithAutoUnseal(),
			admin.WithStorageNodeWatcherOptions(
				snwatcher.WithReportInterval(snwatcher.DefaultTick),
			),
		),
	)
	defer clus.Close(t)

	tpid := clus.TopicIDs()[0]
	client := clus.GetVMSClient(t)

	lsds, err := client.ListLogStreams(context.Background(), tpid)
	assert.NoError(t, err)
	assert.Len(t, lsds, 1)

	_, err = client.AddLogStream(context.Background(), tpid, nil)
	assert.NoError(t, err)

	lsds, err = client.ListLogStreams(context.Background(), tpid)
	assert.NoError(t, err)

	for _, lsd := range lsds {
		assert.Equal(t, varlogpb.LogStreamStatusRunning, lsd.Status)
	}
}

func TestSealUnseal(t *testing.T) {
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithNumberOfTopics(1),
	)
	defer clus.Close(t)

	topicID := clus.TopicIDs()[0]
	lsID := clus.LogStreamIDs(topicID)[0]

	rsp, err := clus.GetVMSClient(t).DescribeTopic(context.Background(), topicID)
	require.NoError(t, err)
	require.Equal(t, topicID, rsp.Topic.TopicID)
	require.Len(t, rsp.Topic.LogStreams, 1)
	require.Len(t, rsp.LogStreams, 1)
	require.Equal(t, topicID, rsp.LogStreams[0].TopicID)
	require.Equal(t, rsp.Topic.LogStreams[0], rsp.LogStreams[0].LogStreamID)
	require.Equal(t, varlogpb.LogStreamStatusRunning, rsp.LogStreams[0].Status)

	_, err = clus.GetVMSClient(t).Seal(context.Background(), topicID, lsID)
	require.NoError(t, err)

	// FIXME: The status of the log stream should be changed to be sealed instantly rather than
	// waiting for it.
	// We expect that calling Seal RPC must change the status of the log stream to be sealed.
	// However, the RPC does not work as our expectation. Here, we wait for the log stream to be
	// sealed for some time.
	require.Eventually(t, func() bool {
		rsp, err = clus.GetVMSClient(t).DescribeTopic(context.Background(), topicID)
		require.NoError(t, err)
		require.Equal(t, topicID, rsp.Topic.TopicID)
		require.Len(t, rsp.Topic.LogStreams, 1)
		require.Len(t, rsp.LogStreams, 1)
		require.Equal(t, topicID, rsp.LogStreams[0].TopicID)
		require.Equal(t, rsp.Topic.LogStreams[0], rsp.LogStreams[0].LogStreamID)
		require.Equal(t, lsID, rsp.LogStreams[0].LogStreamID)
		return rsp.LogStreams[0].Status == varlogpb.LogStreamStatusSealed
	}, 3*time.Second, 100*time.Millisecond)

	_, err = clus.GetVMSClient(t).Unseal(context.Background(), topicID, lsID)
	require.NoError(t, err)

	rsp, err = clus.GetVMSClient(t).DescribeTopic(context.Background(), topicID)
	require.NoError(t, err)
	require.Equal(t, topicID, rsp.Topic.TopicID)
	require.Len(t, rsp.Topic.LogStreams, 1)
	require.Len(t, rsp.LogStreams, 1)
	require.Equal(t, topicID, rsp.LogStreams[0].TopicID)
	require.Equal(t, rsp.Topic.LogStreams[0], rsp.LogStreams[0].LogStreamID)
	require.Equal(t, varlogpb.LogStreamStatusRunning, rsp.LogStreams[0].Status)
}

func TestSyncLogStream(t *testing.T) {
	const numLogs = 100

	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		it.WithNumberOfTopics(1),
	}

	Convey("Given LogStream", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		topicID := env.TopicIDs()[0]
		client := env.ClientAtIndex(t, 0)
		for i := 0; i < numLogs; i++ {
			res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
			So(res.Err, ShouldBeNil)
		}

		Convey("Seal", func(ctx C) {
			lsID := env.LogStreamID(t, topicID, 0)
			rsp, err := env.GetVMSClient(t).Seal(context.Background(), topicID, lsID)
			So(err, ShouldBeNil)
			So(rsp.GetSealedGLSN(), ShouldEqual, types.GLSN(numLogs))

			Convey("Update LS", func(ctx C) {
				newSNID := env.AddSN(t)
				rds := env.ReplicasOf(t, lsID)
				victimSNID := rds[len(rds)-1].GetStorageNodeID()

				// test if victimSNID exists in the logstream and newSNID does not exist
				// in the log stream
				meta, err := env.MRClientAt(t, 0).GetMetadata(context.Background())
				So(err, ShouldBeNil)
				snidmap := make(map[types.StorageNodeID]bool)
				for _, replica := range meta.GetLogStream(lsID).GetReplicas() {
					snidmap[replica.GetStorageNodeID()] = true
				}
				So(snidmap, ShouldNotContainKey, newSNID)
				So(snidmap, ShouldContainKey, victimSNID)

				// update LS
				env.UpdateLS(t, topicID, lsID, victimSNID, newSNID)

				// test if victimSNID does not exist in the logstream and newSNID exists
				// in the log stream
				meta, err = env.MRClientAt(t, 0).GetMetadata(context.Background())
				So(err, ShouldBeNil)
				snidmap = make(map[types.StorageNodeID]bool)
				for _, replica := range meta.GetLogStream(lsID).GetReplicas() {
					snidmap[replica.GetStorageNodeID()] = true
				}
				So(snidmap, ShouldContainKey, newSNID)
				So(snidmap, ShouldNotContainKey, victimSNID)

				Convey("Then it should be synced", func(ctx C) {
					So(testutil.CompareWaitN(200, func() bool {
						snmd, err := env.SNClientOf(t, newSNID).GetMetadata(context.Background())
						if err != nil {
							return false
						}
						lsmd, exist := snmd.GetLogStream(lsID)
						if !exist {
							return false
						}

						rsp, err := env.ReportCommitterClientOf(t, newSNID).GetReport()
						if err != nil {
							return false
						}

						// Log stream replica does not send a report in the learning state.
						rpts := rsp.GetUncommitReports()
						if len(rpts) < 1 {
							return false
						}

						rpt := rpts[0]
						return rpt.GetVersion() == types.Version(numLogs) &&
							rpt.GetUncommittedLLSNOffset() == types.LLSN(numLogs+1) &&
							rpt.GetUncommittedLLSNLength() == 0 &&
							lsmd.Status == varlogpb.LogStreamStatusSealed
					}), ShouldBeTrue)

					_, err := env.GetVMSClient(t).Unseal(context.Background(), topicID, lsID)
					So(err, ShouldBeNil)
				})
			})
		})
	}))
}

func TestSyncLogStreamWithAutoUnseal(t *testing.T) {
	const (
		numLogs          = 10
		tick             = time.Second
		reportInterval   = tick
		heartbeatTimeout = 5 * tick
	)

	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		it.WithNumberOfTopics(1),
		it.WithVMSOptions(
			admin.WithAutoUnseal(),
			admin.WithStorageNodeWatcherOptions(
				snwatcher.WithTick(tick),
				snwatcher.WithHeartbeatTimeout(heartbeatTimeout),
				snwatcher.WithReportInterval(reportInterval),
			),
		),
	)
	defer clus.Close(t)

	tpid := clus.TopicIDs()[0]
	lsid := clus.LogStreamID(t, tpid, 0)

	client := clus.ClientAtIndex(t, 0)
	adm := clus.GetVMSClient(t)

	for i := 0; i < numLogs; i++ {
		res := client.Append(context.Background(), tpid, [][]byte{[]byte("foo")})
		assert.NoError(t, res.Err)
	}

	newsnid := clus.AddSN(t)
	replicas := clus.ReplicasOf(t, lsid)
	oldsnid := replicas[len(replicas)-1].StorageNodeID

	clus.CloseSN(t, oldsnid)
	assert.Eventually(t, func() bool {
		lsd, err := adm.GetLogStream(context.Background(), tpid, lsid)
		assert.NoError(t, err)
		return lsd.Status.Sealed()
	}, heartbeatTimeout*2, tick)

	// test if oldsnid exists in the logstream and newsnid does not exist
	// in the log stream.
	lsd, err := adm.GetLogStream(context.Background(), tpid, lsid)
	assert.NoError(t, err)
	assert.False(t, lsd.IsReplica(newsnid))
	assert.True(t, lsd.IsReplica(oldsnid))

	// update log stream
	clus.UpdateLS(t, tpid, lsid, oldsnid, newsnid)

	// test if oldsnid does not exist in the logstream and newsnid exists
	// in the log stream.
	lsd, err = adm.GetLogStream(context.Background(), tpid, lsid)
	assert.NoError(t, err)
	assert.True(t, lsd.IsReplica(newsnid))
	assert.False(t, lsd.IsReplica(oldsnid))

	// Wait for synchronization of logs and unsealing new replica.
	assert.Eventually(t, func() bool {
		snmd, err := adm.GetStorageNode(context.Background(), newsnid)
		assert.NoError(t, err)

		lsrmd, ok := snmd.GetLogStream(lsid)
		assert.True(t, ok)
		return lsrmd.LocalHighWatermark.GLSN == types.GLSN(numLogs) &&
			lsrmd.Status == varlogpb.LogStreamStatusRunning
	}, 10*tick, tick)
}

func TestSealLogStreamSealedIncompletely(t *testing.T) {
	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		it.WithNumberOfTopics(1),
	}

	Convey("Given cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		Convey("When Seal is incomplete", func() {
			// control failedSN for making test condition
			var failedSNID types.StorageNodeID
			for snID := range env.StorageNodes() {
				failedSNID = snID
				break
			}
			failedSN := env.SNClientOf(t, failedSNID)

			// remove replica to make Seal LS incomplete
			topicID := env.TopicIDs()[0]
			lsID := env.LogStreamIDs(topicID)[0]
			err := failedSN.RemoveLogStream(context.TODO(), topicID, lsID)
			So(err, ShouldBeNil)

			vmsCL := env.GetVMSClient(t)
			rsp, err := vmsCL.Seal(context.TODO(), topicID, lsID)
			// So(err, ShouldNotBeNil)
			So(err, ShouldBeNil)
			So(len(rsp.GetLogStreams()), ShouldBeLessThan, env.ReplicationFactor())

			Convey("Then SN Watcher makes LS sealed", func() {
				snmeta, err := failedSN.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				path := snmeta.GetStorages()[0].GetPath()
				So(len(path), ShouldBeGreaterThan, 0)

				_, err = failedSN.AddLogStreamReplica(context.TODO(), topicID, lsID, path)
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(100, func() bool {
					meta, err := env.GetVMS().Metadata(context.TODO()) //nolint:staticcheck
					if err != nil {
						return false
					}
					ls := meta.GetLogStream(lsID)
					if ls == nil {
						return false
					}

					return ls.Status.Sealed()
				}), ShouldBeTrue)
			})
		})
	}))
}

func TestUnsealLogStreamUnsealedIncompletely(t *testing.T) {
	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		it.WithNumberOfTopics(1),
	}

	Convey("Given Sealed LogStream", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		topicID := env.TopicIDs()[0]
		lsID := env.LogStreamIDs(topicID)[0]

		vmsCL := env.GetVMSClient(t)
		_, err := vmsCL.Seal(context.TODO(), topicID, lsID)
		So(err, ShouldBeNil)

		Convey("When Unseal is incomplete", func(ctx C) {
			// control failedSN for making test condition
			var failedSNID types.StorageNodeID
			for snID := range env.StorageNodes() {
				failedSNID = snID
				break
			}
			failedSN := env.SNClientOf(t, failedSNID)

			// remove replica to make Unseal LS incomplete
			err := failedSN.RemoveLogStream(context.TODO(), topicID, lsID)
			So(err, ShouldBeNil)

			_, err = vmsCL.Unseal(context.TODO(), topicID, lsID)
			So(err, ShouldNotBeNil)

			Convey("Then SN Watcher make LS sealed", func(ctx C) {
				snmeta, err := failedSN.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				path := snmeta.GetStorages()[0].GetPath()
				So(len(path), ShouldBeGreaterThan, 0)

				_, err = failedSN.AddLogStreamReplica(context.TODO(), topicID, lsID, path)
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(100, func() bool {
					meta, err := failedSN.GetMetadata(context.TODO())
					if err != nil {
						return false
					}

					for _, r := range meta.LogStreamReplicas {
						return r.Status.Sealed()
					}

					return false
				}), ShouldBeTrue)
			})
		})
	}))
}

func TestGCZombieLogStream(t *testing.T) {
	const (
		tick             = 100 * time.Millisecond
		reportInterval   = 10 * tick
		heartbeatTimeout = 30 * tick
		gcTimeout        = 6 * reportInterval
	)
	vmsOpts := it.NewTestVMSOptions(
		admin.WithStorageNodeWatcherOptions(
			snwatcher.WithTick(tick),
			snwatcher.WithHeartbeatTimeout(heartbeatTimeout),
			snwatcher.WithReportInterval(reportInterval),
		),
		admin.WithLogStreamGCTimeout(gcTimeout),
	)
	opts := []it.Option{
		it.WithNumberOfStorageNodes(1),
		it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		it.WithVMSOptions(vmsOpts...),
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		snID := env.StorageNodeIDAtIndex(t, 0)
		lsID := types.LogStreamID(1)
		topicID := env.TopicIDs()[0]

		Convey("When AddLogStream to SN but do not register MR", func(ctx C) {
			snMCL := env.SNClientOf(t, snID)

			meta, err := snMCL.GetMetadata(context.TODO())
			So(err, ShouldBeNil)

			path := meta.GetStorages()[0].GetPath()
			_, err = snMCL.AddLogStreamReplica(context.TODO(), topicID, lsID, path)
			So(err, ShouldBeNil)

			meta, err = snMCL.GetMetadata(context.TODO())
			So(err, ShouldBeNil)

			_, exist := meta.GetLogStream(lsID)
			So(exist, ShouldBeTrue)

			Convey("Then the LogStream should removed after GCTimeout", func(ctx C) {
				time.Sleep(gcTimeout / 2)
				meta, err := snMCL.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				_, exist := meta.GetLogStream(lsID)
				So(exist, ShouldBeTrue)

				So(testutil.CompareWait(func() bool {
					meta, err := snMCL.GetMetadata(context.TODO())
					if err != nil {
						return false
					}
					_, exist := meta.GetLogStream(lsID)
					return !exist
				}, gcTimeout*2), ShouldBeTrue)
			})
		})
	}))
}

func TestAddLogStreamTopic(t *testing.T) {
	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		it.WithNumberOfTopics(10),
		it.WithNumberOfClients(1),
	}

	Convey("Given Topic", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		numLogs := 16

		for _, topicID := range env.TopicIDs() {
			rsp, err := env.GetVMSClient(t).DescribeTopic(context.Background(), topicID)
			require.NoError(t, err)
			require.Equal(t, topicID, rsp.Topic.TopicID)
			require.Len(t, rsp.Topic.LogStreams, 1)
			require.Len(t, rsp.LogStreams, 1)
			require.Equal(t, topicID, rsp.LogStreams[0].TopicID)
			require.Equal(t, rsp.Topic.LogStreams[0], rsp.LogStreams[0].LogStreamID)
			require.Equal(t, varlogpb.LogStreamStatusRunning, rsp.LogStreams[0].Status)
		}

		client := env.ClientAtIndex(t, 0)
		for _, topicID := range env.TopicIDs() {
			for i := 0; i < numLogs; i++ {
				res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
				So(res.Err, ShouldBeNil)
			}
		}

		env.ClientRefresh(t)
		client = env.ClientAtIndex(t, 0)

		Convey("When AddLogStream", func(ctx C) {
			vmsCL := env.GetVMSClient(t)
			for _, topicID := range env.TopicIDs() {
				_, err := vmsCL.AddLogStream(context.TODO(), topicID, nil)
				So(err, ShouldBeNil)
			}

			for _, topicID := range env.TopicIDs() {
				rsp, err := env.GetVMSClient(t).DescribeTopic(context.Background(), topicID)
				require.NoError(t, err)
				require.Equal(t, topicID, rsp.Topic.TopicID)
				require.Len(t, rsp.Topic.LogStreams, 2)
				require.Len(t, rsp.LogStreams, 2)
				require.Equal(t, topicID, rsp.LogStreams[0].TopicID)
				require.Equal(t, topicID, rsp.LogStreams[1].TopicID)
				require.Equal(t, varlogpb.LogStreamStatusRunning, rsp.LogStreams[0].Status)
				require.Equal(t, varlogpb.LogStreamStatusRunning, rsp.LogStreams[1].Status)
			}

			Convey("Then it should Appendable", func(ctx C) {
				env.ClientRefresh(t)
				client = env.ClientAtIndex(t, 0)

				for _, topicID := range env.TopicIDs() {
					rsp, _ := env.GetVMSClient(t).DescribeTopic(context.Background(), topicID)
					// AppendTo newbie LogStream
					res := client.AppendTo(context.Background(), topicID, rsp.LogStreams[1].LogStreamID, [][]byte{[]byte("foo")})
					So(res.Err, ShouldBeNil)

					for i := 0; i < numLogs; i++ {
						res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
						So(res.Err, ShouldBeNil)
					}
				}
			})
		})
	}))
}

func TestRemoveTopic(t *testing.T) {
	t.Skip()

	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(2),
		it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		it.WithNumberOfTopics(10),
		it.WithNumberOfClients(1),
	}

	Convey("Given Topic", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		numLogs := 8

		client := env.ClientAtIndex(t, 0)
		for _, topicID := range env.TopicIDs() {
			for i := 0; i < numLogs; i++ {
				res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
				So(res.Err, ShouldBeNil)
			}
		}

		Convey("When RemoveTopic", func(ctx C) {
			vmsCL := env.GetVMSClient(t)
			rmTopicID := env.TopicIDs()[0]
			err := vmsCL.UnregisterTopic(context.TODO(), rmTopicID)
			So(err, ShouldBeNil)

			meta := env.GetMetadata(t)
			So(meta.GetTopic(rmTopicID), ShouldBeNil)

			Convey("Then unregistered topic should be Unappendable", func(ctx C) {
				actx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				res := client.Append(actx, rmTopicID, [][]byte{[]byte("foo")})
				So(res.Err, ShouldNotBeNil)

				Convey("And other topics should Appendable", func(ctx C) {
					for _, topicID := range env.TopicIDs() {
						if topicID == rmTopicID {
							continue
						}

						for i := 0; i < numLogs; i++ {
							res := client.Append(context.Background(), topicID, [][]byte{[]byte("foo")})
							So(res.Err, ShouldBeNil)
						}
					}
				})
			})
		})
	}))
}

func TestAddTopic(t *testing.T) {
	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(2),
		it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		it.WithNumberOfTopics(3),
	}

	Convey("Given Topic", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		testTimeout := 5 * time.Second

		tctx, tcancel := context.WithTimeout(context.TODO(), testTimeout)
		defer tcancel()

		grp, gctx := errgroup.WithContext(tctx)
		for _, topicID := range env.TopicIDs() {
			tid := topicID
			grp.Go(func() (err error) {
				cl, err := varlog.Open(context.Background(), env.ClusterID(), env.MRRPCEndpoints())
				if err != nil {
					return err
				}
				defer cl.Close()

				for gctx.Err() == nil {
					res := cl.Append(context.Background(), tid, [][]byte{[]byte("foo")})
					if res.Err != nil {
						err = fmt.Errorf("topic=%v,err=%v", tid, res.Err)
						break
					}
				}
				return
			})
		}

		Convey("When AddTopic", func(ctx C) {
			vmsCL := env.GetVMSClient(t)
			topicDesc, err := vmsCL.AddTopic(context.TODO())
			So(err, ShouldBeNil)

			addTopicID := topicDesc.TopicID

			tds, err := vmsCL.ListTopics(context.TODO())
			So(err, ShouldBeNil)
			So(tds, ShouldHaveLength, 4)

			_, err = vmsCL.AddLogStream(context.TODO(), addTopicID, nil)
			So(err, ShouldBeNil)

			grp.Go(func() (err error) {
				cl, err := varlog.Open(context.Background(), env.ClusterID(), env.MRRPCEndpoints())
				if err != nil {
					return err
				}
				defer cl.Close()

				for gctx.Err() == nil {
					res := cl.Append(context.Background(), addTopicID, [][]byte{[]byte("foo")})
					if res.Err != nil {
						err = fmt.Errorf("topic=%v,err=%v", addTopicID, res.Err)
						break
					}
				}
				return
			})

			Convey("Then it should appendable", func(ctx C) {
				err = grp.Wait()
				So(err, ShouldBeNil)
			})
		})
	}))
}
