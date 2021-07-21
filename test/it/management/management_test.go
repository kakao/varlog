package management

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/test/it"
)

func TestUnregisterInactiveStorageNode(t *testing.T) {
	clus := it.NewVarlogCluster(t, it.WithNumberOfStorageNodes(1))
	defer clus.Close(t)

	snID := clus.StorageNodeIDAtIndex(t, 0)
	_, err := clus.GetVMSClient(t).UnregisterStorageNode(context.Background(), snID)
	require.NoError(t, err)

	rsp, err := clus.GetVMSClient(t).GetStorageNodes(context.Background())
	require.NoError(t, err)
	require.Len(t, rsp.GetStoragenodes(), 0)
}

func TestUnregisterActiveStorageNode(t *testing.T) {
	clus := it.NewVarlogCluster(t, it.WithNumberOfStorageNodes(1), it.WithNumberOfLogStreams(1))
	defer clus.Close(t)

	snID := clus.StorageNodeIDAtIndex(t, 0)
	_, err := clus.GetVMSClient(t).UnregisterStorageNode(context.Background(), snID)
	require.Error(t, err)
}

func TestAddAlreadyExistedStorageNode(t *testing.T) {
	clus := it.NewVarlogCluster(t, it.WithNumberOfStorageNodes(1))
	defer clus.Close(t)

	snID := clus.StorageNodeIDAtIndex(t, 0)
	addr := clus.SNClientOf(t, snID).PeerAddress()
	_, err := clus.GetVMSClient(t).AddStorageNode(context.TODO(), addr)
	require.Error(t, err)
}

func TestUnregisterLogStream(t *testing.T) {
	clus := it.NewVarlogCluster(t, it.WithNumberOfStorageNodes(1), it.WithNumberOfLogStreams(1))
	defer clus.Close(t)

	lsID := clus.LogStreamIDs()[0]
	_, err := clus.GetVMSClient(t).UnregisterLogStream(context.Background(), lsID)
	require.Error(t, err)

	_, err = clus.GetVMSClient(t).Seal(context.Background(), lsID)
	require.NoError(t, err)

	_, err = clus.GetVMSClient(t).UnregisterLogStream(context.Background(), lsID)
	require.NoError(t, err)
}

func TestAddLogStreamWithNotExistedNode(t *testing.T) {
	clus := it.NewVarlogCluster(t)
	defer clus.Close(t)

	replicas := []*varlogpb.ReplicaDescriptor{
		{
			StorageNodeID: types.StorageNodeID(1),
			Path:          "/fake",
		},
	}
	_, err := clus.GetVMSClient(t).AddLogStream(context.Background(), replicas)
	require.Error(t, err)
}

func TestAddLogStreamManually(t *testing.T) {
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
	)
	defer clus.Close(t)

	replicas := make([]*varlogpb.ReplicaDescriptor, 0, clus.ReplicationFactor())
	for snID := range clus.StorageNodes() {
		snmd, err := clus.SNClientOf(t, snID).GetMetadata(context.Background())
		require.NoError(t, err)

		replicas = append(replicas, &varlogpb.ReplicaDescriptor{
			StorageNodeID: snID,
			Path:          snmd.GetStorageNode().GetStorages()[0].GetPath(),
		})
	}

	_, err := clus.GetVMSClient(t).AddLogStream(context.Background(), replicas)
	require.NoError(t, err)
}

func TestAddLogStreamPartiallyRegistered(t *testing.T) {
	const lsID = types.LogStreamID(1)

	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
	)
	defer clus.Close(t)

	// NOTE: Log stream whose ID is 1 is partially generated.
	// SN1 has a log stream replica whose ID is 1.
	// SN2 has no log stream replica.
	snid1 := clus.StorageNodeIDAtIndex(t, 0)
	sn1 := clus.SNClientOf(t, snid1)
	snmd1, err := sn1.GetMetadata(context.Background())
	require.NoError(t, err)
	err = sn1.AddLogStream(context.Background(), lsID, snmd1.GetStorageNode().GetStorages()[0].GetPath())
	require.NoError(t, err)

	snid2 := clus.StorageNodeIDAtIndex(t, 1)
	sn2 := clus.SNClientOf(t, snid2)
	snmd2, err := sn2.GetMetadata(context.Background())
	require.NoError(t, err)

	// NOTE: VMS tries to create new log stream replica, and its ID will be 1.
	// But the log stream was generated partially.
	replicas := []*varlogpb.ReplicaDescriptor{
		{
			StorageNodeID: snid1,
			Path:          snmd1.GetStorageNode().GetStorages()[0].GetPath(),
		},
		{
			StorageNodeID: snid2,
			Path:          snmd2.GetStorageNode().GetStorages()[0].GetPath(),
		},
	}
	_, err = clus.GetVMSClient(t).AddLogStream(context.Background(), replicas)
	require.Error(t, err)

	// Retring add new log stream will be succeed, since VMS refreshes its ID pool.
	_, err = clus.GetVMSClient(t).AddLogStream(context.Background(), replicas)
	require.NoError(t, err)
}

func TestRemoveLogStreamReplica(t *testing.T) {
	const lsID = types.LogStreamID(1)

	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
	)
	defer clus.Close(t)

	// Not registered log stream replica: garbage
	snid := clus.StorageNodeIDAtIndex(t, 0)
	sn := clus.SNClientOf(t, snid)
	snmd, err := sn.GetMetadata(context.Background())
	require.NoError(t, err)
	err = sn.AddLogStream(context.Background(), lsID, snmd.GetStorageNode().GetStorages()[0].GetPath())
	require.NoError(t, err)

	_, err = clus.GetVMSClient(t).RemoveLogStreamReplica(context.TODO(), snid, lsID)
	require.NoError(t, err)
}

func TestSealUnseal(t *testing.T) {
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
	)
	defer clus.Close(t)

	lsID := clus.LogStreamIDs()[0]

	_, err := clus.GetVMSClient(t).Seal(context.Background(), lsID)
	require.NoError(t, err)

	_, err = clus.GetVMSClient(t).Unseal(context.Background(), lsID)
	require.NoError(t, err)
}

func TestSyncLogStream(t *testing.T) {
	const numLogs = 100

	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithReporterClientFactory(metadata_repository.NewReporterClientFactory()),
		it.WithStorageNodeManagementClientFactory(metadata_repository.NewEmptyStorageNodeClientFactory()),
	}

	Convey("Given LogStream", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		client := env.ClientAtIndex(t, 0)
		for i := 0; i < numLogs; i++ {
			_, err := client.Append(context.Background(), []byte("foo"))
			So(err, ShouldBeNil)
		}

		Convey("Seal", func(ctx C) {
			lsID := env.LogStreamID(t, 0)
			rsp, err := env.GetVMSClient(t).Seal(context.Background(), lsID)
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
				env.UpdateLS(t, lsID, victimSNID, newSNID)

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
						lsmd, exist := snmd.FindLogStream(lsID)
						if !exist {
							return false
						}

						rsp, err := env.ReportCommitterClientOf(t, newSNID).GetReport()
						if err != nil {
							return false
						}
						rpt := rsp.GetUncommitReports()[0]
						return rpt.GetHighWatermark() == types.GLSN(numLogs) &&
							rpt.GetUncommittedLLSNOffset() == types.LLSN(numLogs+1) &&
							rpt.GetUncommittedLLSNLength() == 0 &&
							lsmd.Status == varlogpb.LogStreamStatusSealed
					}), ShouldBeTrue)

					_, err := env.GetVMSClient(t).Unseal(context.Background(), lsID)
					So(err, ShouldBeNil)
				})
			})
		})
	}))
}

func TestSealLogStreamSealedIncompletely(t *testing.T) {
	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithReporterClientFactory(metadata_repository.NewReporterClientFactory()),
		it.WithStorageNodeManagementClientFactory(metadata_repository.NewEmptyStorageNodeClientFactory()),
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

			// remove replica to make Seal LS imcomplete
			lsID := env.LogStreamIDs()[0]
			err := failedSN.RemoveLogStream(context.TODO(), lsID)
			So(err, ShouldBeNil)

			vmsCL := env.GetVMSClient(t)
			rsp, err := vmsCL.Seal(context.TODO(), lsID)
			// So(err, ShouldNotBeNil)
			So(err, ShouldBeNil)
			So(len(rsp.GetLogStreams()), ShouldBeLessThan, env.ReplicationFactor())

			Convey("Then SN Watcher makes LS sealed", func() {
				snmeta, err := failedSN.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				path := snmeta.GetStorageNode().GetStorages()[0].GetPath()
				So(len(path), ShouldBeGreaterThan, 0)

				err = failedSN.AddLogStream(context.TODO(), lsID, path)
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(100, func() bool {
					meta, err := env.GetVMS().Metadata(context.TODO())
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
		it.WithReporterClientFactory(metadata_repository.NewReporterClientFactory()),
		it.WithStorageNodeManagementClientFactory(metadata_repository.NewEmptyStorageNodeClientFactory()),
	}

	Convey("Given Sealed LogStream", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		lsID := env.LogStreamIDs()[0]

		vmsCL := env.GetVMSClient(t)
		_, err := vmsCL.Seal(context.TODO(), lsID)
		So(err, ShouldBeNil)

		Convey("When Unseal is incomplete", func(ctx C) {
			// control failedSN for making test condition
			var failedSNID types.StorageNodeID
			for snID := range env.StorageNodes() {
				failedSNID = snID
				break
			}
			failedSN := env.SNClientOf(t, failedSNID)

			// remove replica to make Unseal LS imcomplete
			err := failedSN.RemoveLogStream(context.TODO(), lsID)
			So(err, ShouldBeNil)

			_, err = vmsCL.Unseal(context.TODO(), lsID)
			So(err, ShouldNotBeNil)

			Convey("Then SN Watcher make LS sealed", func(ctx C) {
				snmeta, err := failedSN.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				path := snmeta.GetStorageNode().GetStorages()[0].GetPath()
				So(len(path), ShouldBeGreaterThan, 0)

				err = failedSN.AddLogStream(context.TODO(), lsID, path)
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(100, func() bool {
					meta, err := failedSN.GetMetadata(context.TODO())
					if err != nil {
						return false
					}

					for _, r := range meta.LogStreams {
						return r.Status.Sealed()
					}

					return false
				}), ShouldBeTrue)
			})
		})
	}))
}

func TestGCZombieLogStream(t *testing.T) {
	vmsOpts := it.NewTestVMSOptions()
	vmsOpts.GCTimeout = 6 * time.Duration(vmsOpts.ReportInterval) * vmsOpts.Tick
	opts := []it.Option{
		it.WithNumberOfStorageNodes(1),
		it.WithReporterClientFactory(metadata_repository.NewReporterClientFactory()),
		it.WithStorageNodeManagementClientFactory(metadata_repository.NewEmptyStorageNodeClientFactory()),
		it.WithVMSOptions(vmsOpts),
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		snID := env.StorageNodeIDAtIndex(t, 0)
		lsID := types.LogStreamID(1)

		Convey("When AddLogStream to SN but do not register MR", func(ctx C) {
			snMCL := env.SNClientOf(t, snID)

			meta, err := snMCL.GetMetadata(context.TODO())
			So(err, ShouldBeNil)

			path := meta.GetStorageNode().GetStorages()[0].GetPath()
			err = snMCL.AddLogStream(context.TODO(), lsID, path)
			So(err, ShouldBeNil)

			meta, err = snMCL.GetMetadata(context.TODO())
			So(err, ShouldBeNil)

			_, exist := meta.FindLogStream(lsID)
			So(exist, ShouldBeTrue)

			Convey("Then the LogStream should removed after GCTimeout", func(ctx C) {
				time.Sleep(vmsOpts.GCTimeout / 2)
				meta, err := snMCL.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				_, exist := meta.FindLogStream(lsID)
				So(exist, ShouldBeTrue)

				So(testutil.CompareWait(func() bool {
					meta, err := snMCL.GetMetadata(context.TODO())
					if err != nil {
						return false
					}
					_, exist := meta.FindLogStream(lsID)
					return !exist
				}, vmsOpts.GCTimeout), ShouldBeTrue)
			})
		})
	}))
}
