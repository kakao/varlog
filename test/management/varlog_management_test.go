package management

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	assert "github.com/smartystreets/assertions"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/internal/vms"
	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/util/testutil/ports"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/test"
	"github.com/kakao/varlog/vtesting"
)

func TestVarlogManagerServer(t *testing.T) {
	vmsOpts := vms.DefaultOptions()
	vmsOpts.Tick = 100 * time.Millisecond
	vmsOpts.HeartbeatTimeout = 30
	vmsOpts.ReportInterval = 10
	vmsOpts.Logger = zap.L()
	opts := test.VarlogClusterOptions{
		NrMR:                  1,
		NrRep:                 3,
		ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
		SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		VMSOpts:               &vmsOpts,
	}

	Convey("AddStorageNode", t, test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
		nrSN := opts.NrRep

		cmcli := env.GetClusterManagerClient()

		var replicas []*varlogpb.ReplicaDescriptor
		snAddrs := make(map[types.StorageNodeID]string, nrSN)

		for i := 0; i < nrSN; i++ {
			snid := env.AddSN(t)
			sn := env.LookupSN(t, snid)
			snmd, err := sn.GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			path := snmd.GetStorageNode().GetStorages()[0].GetPath()
			replica := &varlogpb.ReplicaDescriptor{
				StorageNodeID: snid,
				Path:          path,
			}
			replicas = append(replicas, replica)

			snAddr := snmd.GetStorageNode().GetAddress()
			So(len(snAddr), ShouldBeGreaterThan, 0)
			snAddrs[snid] = snAddr
		}

		Convey("UnregisterStorageNode", func() {
			for snid := range env.StorageNodes() {
				_, err := cmcli.UnregisterStorageNode(context.TODO(), snid)
				So(err, ShouldBeNil)

				clusmeta, err := env.GetMR(t).GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				So(clusmeta.GetStorageNode(snid), ShouldBeNil)
			}

			// AddLogStream: ERROR
			_, err := cmcli.AddLogStream(context.TODO(), replicas)
			So(err, ShouldNotBeNil)
		})

		Convey("Duplicated AddStorageNode", func() {
			for _, snAddr := range snAddrs {
				_, err := cmcli.AddStorageNode(context.TODO(), snAddr)
				So(err, assert.ShouldWrap, verrors.ErrExist)
				So(errors.Is(err, verrors.ErrExist), ShouldBeTrue)
			}
		})

		Convey("AddLogStream - Simple", func() {
			logStreamID := env.AddLS(t)

			Convey("UnregisterLogStream ERROR: running log stream", func() {
				_, err := cmcli.UnregisterLogStream(context.TODO(), logStreamID)
				So(err, ShouldNotBeNil)
			})

			Convey("Seal", func() {
				rsp, err := cmcli.Seal(context.TODO(), logStreamID)
				So(err, ShouldBeNil)
				lsmetaList := rsp.GetLogStreams()
				So(len(lsmetaList), ShouldEqual, opts.NrRep)
				So(lsmetaList[0].HighWatermark, ShouldEqual, types.InvalidGLSN)

				// check MR metadata: sealed
				clusmeta, err := env.GetMR(t).GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				lsdesc := clusmeta.GetLogStream(logStreamID)
				So(lsdesc, ShouldNotBeNil)
				So(lsdesc.GetStatus(), ShouldEqual, varlogpb.LogStreamStatusSealed)
				So(len(lsdesc.GetReplicas()), ShouldEqual, opts.NrRep)

				// check SN metadata: sealed
				for _, sn := range env.StorageNodes() {
					snmeta, err := sn.GetMetadata(context.TODO())
					So(err, ShouldBeNil)
					So(snmeta.GetLogStreams()[0].GetStatus(), ShouldEqual, varlogpb.LogStreamStatusSealed)
				}

				Convey("UnregisterLogStream OK: sealed log stream", func() {
					_, err := cmcli.UnregisterLogStream(context.TODO(), logStreamID)
					So(err, ShouldBeNil)

					clusmeta, err := env.GetMR(t).GetMetadata(context.TODO())
					So(err, ShouldBeNil)

					So(clusmeta.GetLogStream(logStreamID), ShouldBeNil)
				})

				Convey("Unseal", func() {
					_, err := cmcli.Unseal(context.TODO(), logStreamID)
					So(err, ShouldBeNil)

					clusmeta, err := env.GetMR(t).GetMetadata(context.TODO())
					So(err, ShouldBeNil)
					lsdesc := clusmeta.GetLogStream(logStreamID)
					So(lsdesc, ShouldNotBeNil)
					So(lsdesc.GetStatus(), ShouldEqual, varlogpb.LogStreamStatusRunning)
				})
			})
		})

		Convey("AddLogStream - Manual", func() {
			rsp, err := cmcli.AddLogStream(context.TODO(), replicas)
			So(err, ShouldBeNil)
			logStreamDesc := rsp.GetLogStream()
			So(len(logStreamDesc.GetReplicas()), ShouldEqual, opts.NrRep)

			// pass since NrRep equals to NrSN
			var snidList []types.StorageNodeID
			for _, replica := range logStreamDesc.GetReplicas() {
				snidList = append(snidList, replica.GetStorageNodeID())
			}
			for snid := range env.StorageNodes() {
				So(snidList, ShouldContain, snid)
			}

			clusmeta, err := env.GetMR(t).GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			So(clusmeta.GetLogStream(logStreamDesc.GetLogStreamID()), ShouldNotBeNil)
		})

		Convey("AddLogStream - Error: no such StorageNodeID", func() {
			rand.Seed(time.Now().UnixNano())
			i := rand.Intn(len(replicas))
			// invalid storage node id
			replicas[i].StorageNodeID += types.StorageNodeID(nrSN)

			_, err := cmcli.AddLogStream(context.TODO(), replicas)
			So(err, ShouldNotBeNil)
		})

		Convey("AddLogStream - Error: duplicated, but not registered logstream", func() {
			// Add logstream to storagenode
			badSNID := replicas[0].GetStorageNodeID()
			badSN := env.LookupSN(t, badSNID)
			badLSID := types.LogStreamID(1)
			path := replicas[0].GetPath()
			_, err := badSN.AddLogStream(context.TODO(), badLSID, path)
			So(err, ShouldBeNil)

			// Not registered logstream
			clusmeta, err := env.GetMR(t).GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			So(clusmeta.GetLogStream(badLSID), ShouldBeNil)

			// FIXME (jun): This test passes due to seqLSIDGen, but it is very fragile.
			_, err = cmcli.AddLogStream(context.TODO(), replicas)
			So(err, ShouldNotBeNil)
			So(verrors.IsTransient(err), ShouldBeTrue)

			Convey("RemoveLogStreamReplica: garbage log stream can be removed", func() {
				_, err := cmcli.RemoveLogStreamReplica(context.TODO(), badSNID, types.LogStreamID(1))
				So(err, ShouldBeNil)
			})

			Convey("AddLogStream - OK: due to LogStreamIDGenerator.Refresh", func() {
				rsp, err := cmcli.AddLogStream(context.TODO(), replicas)
				So(err, ShouldBeNil)
				logStreamDesc := rsp.GetLogStream()
				So(len(logStreamDesc.GetReplicas()), ShouldEqual, opts.NrRep)

				clusmeta, err := env.GetMR(t).GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				So(clusmeta.GetLogStream(logStreamDesc.GetLogStreamID()), ShouldNotBeNil)

				Convey("RemoveLogStreamReplica: registered log stream cannot be removed", func() {
					logStreamID := logStreamDesc.GetLogStreamID()
					_, err := cmcli.RemoveLogStreamReplica(context.TODO(), badSNID, logStreamID)
					So(err, ShouldNotBeNil)
				})
			})
		})
	}))
}

func TestVarlogNewMRManager(t *testing.T) {
	Convey("Given that MRManager runs without any running MR", t, func(c C) {
		const (
			clusterID = types.ClusterID(1)
			portBase  = 20000
		)

		portLease, err := ports.ReserveWeaklyWithRetry(portBase)
		So(err, ShouldBeNil)

		var (
			mrRAFTAddr = fmt.Sprintf("http://127.0.0.1:%d", portLease.Base())
			mrRPCAddr  = fmt.Sprintf("127.0.0.1:%d", portLease.Base()+1)
		)

		defer func() {
			So(portLease.Release(), ShouldBeNil)
		}()

		vmsOpts := vms.DefaultOptions()

		Convey("When MR does not start within specified periods", func(c C) {
			vmsOpts.InitialMRConnRetryCount = 1
			vmsOpts.InitialMRConnRetryBackoff = 100 * time.Millisecond
			vmsOpts.MetadataRepositoryAddresses = []string{mrRPCAddr}

			Convey("Then the MRManager should return an error", func(ctx C) {
				_, err := vms.NewMRManager(context.TODO(), clusterID, vmsOpts.MRManagerOptions, zap.L())
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When MR starts within specified periods", func(c C) {
			vmsOpts.InitialMRConnRetryCount = 30
			vmsOpts.InitialMRConnRetryBackoff = 1 * time.Second
			vmsOpts.MetadataRepositoryAddresses = []string{mrRPCAddr}

			// vms
			mrmC := make(chan vms.MetadataRepositoryManager, 1)
			go func() {
				defer close(mrmC)
				mrm, err := vms.NewMRManager(context.TODO(), clusterID, vmsOpts.MRManagerOptions, zap.L())
				if err != nil {
					fmt.Printf("err: %+v\n", err)
				}
				c.So(err, ShouldBeNil)
				if err == nil {
					mrmC <- mrm
				}
			}()

			time.Sleep(10 * time.Millisecond)

			// mr
			mrOpts := &metadata_repository.MetadataRepositoryOptions{
				RaftOptions: metadata_repository.RaftOptions{
					Join:      false,
					SnapCount: uint64(10),
					RaftTick:  vtesting.TestRaftTick(),
					RaftDir:   filepath.Join(t.TempDir(), "raftdir"),
					Peers:     []string{mrRAFTAddr},
				},

				ClusterID:                      clusterID,
				RaftAddress:                    mrRAFTAddr,
				LogDir:                         filepath.Join(t.TempDir(), "log"),
				RPCTimeout:                     vtesting.TimeoutAccordingToProcCnt(metadata_repository.DefaultRPCTimeout),
				NumRep:                         1,
				RPCBindAddress:                 mrRPCAddr,
				ReporterClientFac:              metadata_repository.NewReporterClientFactory(),
				StorageNodeManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
				Logger:                         zap.L(),
			}
			mr := metadata_repository.NewRaftMetadataRepository(mrOpts)
			mr.Run()

			Reset(func() {
				So(mr.Close(), ShouldBeNil)
			})

			Convey("Then the MRManager should connect to the MR", func(ctx C) {
				mrm, ok := <-mrmC
				So(ok, ShouldBeTrue)
				So(mrm.Close(), ShouldBeNil)
			})
		})

	})

	Convey("Given MR cluster", t, func(ctx C) {
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 1,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}
		vmsOpts := vms.DefaultOptions()
		vmsOpts.InitialMRConnRetryCount = 3
		opts.VMSOpts = &vmsOpts
		env := test.NewVarlogCluster(t, opts)
		env.Start(t)
		defer env.Close(t)

		mr := env.GetMR(t)
		So(testutil.CompareWaitN(50, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		Convey("When create MRManager with non addrs", func(ctx C) {
			opts.VMSOpts.MRManagerOptions.MetadataRepositoryAddresses = nil
			// VMS Server
			_, err := vms.NewMRManager(context.TODO(), types.ClusterID(0), opts.VMSOpts.MRManagerOptions, zap.L())
			Convey("Then it should be fail", func(ctx C) {
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When create MRManager with invalid addrs", func(ctx C) {
			// VMS Server
			opts.VMSOpts.MRManagerOptions.MetadataRepositoryAddresses = []string{fmt.Sprintf("%s%d", mrAddr, 0)}
			_, err := vms.NewMRManager(context.TODO(), types.ClusterID(0), opts.VMSOpts.MRManagerOptions, zap.L())
			Convey("Then it should be fail", func(ctx C) {
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When create MRManager with valid addrs", func(ctx C) {
			// VMS Server
			opts.VMSOpts.MRManagerOptions.MetadataRepositoryAddresses = []string{mrAddr}
			mrm, err := vms.NewMRManager(context.TODO(), types.ClusterID(0), opts.VMSOpts.MRManagerOptions, zap.L())
			Convey("Then it should be success", func(ctx C) {
				So(err, ShouldBeNil)

				Convey("and it should work", func(ctx C) {
					cinfo, err := mrm.GetClusterInfo(context.TODO())
					So(err, ShouldBeNil)
					So(len(cinfo.GetMembers()), ShouldEqual, 1)
				})
			})
			defer mrm.Close()
		})
	})
}

func TestVarlogMRManagerWithLeavedNode(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 1,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}
		vmsOpts := vms.DefaultOptions()
		opts.VMSOpts = &vmsOpts
		env := test.NewVarlogCluster(t, opts)
		env.Start(t)
		defer env.Close(t)

		mr := env.GetMR(t)
		So(testutil.CompareWaitN(50, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		// VMS Server
		opts.VMSOpts.MRManagerOptions.MetadataRepositoryAddresses = []string{mrAddr}
		mrm, err := vms.NewMRManager(context.TODO(), types.ClusterID(0), opts.VMSOpts.MRManagerOptions, zap.L())
		So(err, ShouldBeNil)
		defer mrm.Close()

		Convey("When all the cluster configuration nodes are changed", func(ctx C) {
			env.AppendMR(t)
			env.StartMR(t, 1)

			nmr := env.GetMRByIndex(t, 1)

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err = mrm.AddPeer(rctx, env.MRIDs[1], env.MRPeers[1], env.MRRPCEndpoints[1])
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(50, func() bool {
				return nmr.IsMember()
			}), ShouldBeTrue)

			rctx, cancel = context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err = mrm.RemovePeer(rctx, env.MRIDs[0])
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(50, func() bool {
				return !mr.IsMember()
			}), ShouldBeTrue)

			oldCL, err := mrc.NewMetadataRepositoryManagementClient(context.TODO(), mrAddr)
			So(err, ShouldBeNil)
			_, err = oldCL.GetClusterInfo(context.TODO(), types.ClusterID(0))
			So(errors.Is(err, verrors.ErrNotMember), ShouldBeTrue)
			So(oldCL.Close(), ShouldBeNil)

			So(testutil.CompareWaitN(50, func() bool {
				cinfo, err := nmr.GetClusterInfo(context.TODO(), types.ClusterID(0))
				return err == nil && len(cinfo.GetMembers()) == 1 && cinfo.GetLeader() == cinfo.GetNodeID()
			}), ShouldBeTrue)

			Convey("Then it should be success", func(ctx C) {
				rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
				defer cancel()

				cinfo, err := mrm.GetClusterInfo(rctx)
				So(err, ShouldBeNil)
				So(len(cinfo.GetMembers()), ShouldEqual, 1)
			})
		})
	})
}
